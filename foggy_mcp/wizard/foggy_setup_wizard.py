# -*- coding: utf-8 -*-
import logging
import os
import platform
import secrets
import shlex
import subprocess
import json

from odoo import _, api, fields, models, tools

_logger = logging.getLogger(__name__)

# Docker image with built-in Odoo models
FOGGY_DOCKER_IMAGE = 'foggysource/foggy-odoo-mcp:v8.1.8-beta'

# All Odoo query models (built into Docker image)
QUERY_MODELS = [
    'OdooSaleOrderQueryModel',
    'OdooSaleOrderLineQueryModel',
    'OdooPurchaseOrderQueryModel',
    'OdooAccountMoveQueryModel',
    'OdooStockPickingQueryModel',
    'OdooHrEmployeeQueryModel',
    'OdooResPartnerQueryModel',
    'OdooResCompanyQueryModel',
    'OdooCrmLeadQueryModel',
]


def _get_docker_network_info():
    """
    Detect if running in Docker and get network information.

    Returns:
        dict: {
            'in_docker': bool,
            'container_id': str or None,
            'network_name': str or None,
            'network_mode': str or None,  # 'bridge', 'host', 'none', or custom network name
            'error': str or None
        }
    """
    result = {
        'in_docker': False,
        'container_id': None,
        'network_name': None,
        'network_mode': None,
        'error': None,
    }

    try:
        # Check if we're inside a Docker container
        # Method 1: Check /.dockerenv file
        if os.path.exists('/.dockerenv'):
            result['in_docker'] = True

        # Method 2: Check /proc/1/cgroup for docker signature (cgroup v1)
        if not result['in_docker']:
            try:
                with open('/proc/1/cgroup', 'r') as f:
                    cgroup = f.read()
                    if 'docker' in cgroup or 'containerd' in cgroup:
                        result['in_docker'] = True
            except Exception:
                pass

        # Method 3: Check for container environment variable
        if not result['in_docker']:
            # Many container runtimes set this
            if os.environ.get('KUBERNETES_SERVICE_HOST') or os.environ.get('container'):
                result['in_docker'] = True

        if not result['in_docker']:
            return result

        # Get container ID from hostname (usually container ID in Docker)
        try:
            result['container_id'] = os.uname().nodename[:12]
        except Exception:
            pass

        # Try to get network info via Docker socket
        docker_socket = '/var/run/docker.sock'
        if os.path.exists(docker_socket):
            try:
                # Use curl to query Docker API (lighter than installing docker SDK)
                cmd = [
                    'curl', '--silent', '--unix-socket', docker_socket,
                    f'http://localhost/containers/{result["container_id"]}/json'
                ]
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                if proc.returncode == 0 and proc.stdout:
                    container_info = json.loads(proc.stdout)

                    # Get network settings
                    network_settings = container_info.get('NetworkSettings', {})
                    networks = network_settings.get('Networks', {})

                    if networks:
                        # Get the first network (usually the main one)
                        for net_name, net_config in networks.items():
                            if net_name not in ('bridge', 'host', 'none'):
                                # Custom network (like foggy-odoo_default)
                                result['network_name'] = net_name
                                result['network_mode'] = 'custom'
                                break
                            else:
                                result['network_name'] = net_name
                                result['network_mode'] = net_name
                                break

                    # Also check HostConfig.NetworkMode
                    host_config = container_info.get('HostConfig', {})
                    network_mode = host_config.get('NetworkMode', '')
                    if network_mode and network_mode not in ('default', ''):
                        result['network_mode'] = network_mode
                        if network_mode not in ('bridge', 'host', 'none'):
                            result['network_name'] = network_mode

            except Exception as e:
                result['error'] = f"Docker API error: {e}"
        else:
            # Docker socket not mounted - try alternative methods
            result['error'] = "Docker socket not mounted"

            # Method 1: Check if db_host is a Docker service name (not IP/localhost)
            # This is the most reliable indicator of Docker networking
            try:
                db_host = tools.config.get('db_host', '')
                if db_host and db_host not in ('localhost', '127.0.0.1', 'False', False, ''):
                    # db_host is a hostname like 'postgres' - definitely Docker network
                    # We can't know the exact network name without Docker API,
                    # but we know we're in a Docker network
                    result['network_name'] = 'docker_network'  # Placeholder
                    result['network_mode'] = 'custom'
                    result['error'] = None  # Clear error
                    _logger.info("Detected Docker network via db_host: %s", db_host)
            except Exception as e:
                _logger.warning("Failed to detect network via db_host: %s", e)

    except Exception as e:
        result['error'] = str(e)

    _logger.info("Docker network detection: %s", result)
    return result


class FoggySetupWizard(models.TransientModel):
    _name = 'foggy.setup.wizard'
    _description = 'Foggy MCP Setup Wizard'

    # ── Step control ──────────────────────────────────────────────────

    state = fields.Selection([
        ('welcome', 'Welcome'),
        ('deploy', 'Deploy'),
        ('connection', 'Connection'),
        ('datasource', 'Data Source'),
        ('closure', 'Closure Tables'),
        ('done', 'Done'),
    ], string='Step', default='welcome', required=True)

    # 引擎模式（继承自 Settings，向导中也可选择）
    engine_mode = fields.Selection([
        ('gateway', 'Gateway'),
        ('embedded', 'Embedded (Recommended)'),
    ], string='Engine Mode', default='embedded')

    embedded_available = fields.Boolean(
        string='Embedded Engine Available', compute='_compute_embedded_available')
    embedded_status = fields.Text(string='Embedded Engine Status', readonly=True)

    @api.depends('engine_mode')
    def _compute_embedded_available(self):
        for rec in self:
            try:
                import foggy.mcp_spi  # noqa: F401
                rec.embedded_available = True
            except ImportError:
                rec.embedded_available = False

    # ── Step 2: Deploy config ─────────────────────────────────────────

    foggy_port = fields.Integer(string='Foggy MCP Port', default=7108)
    auth_token = fields.Char(string='Auth Token', readonly=True)

    # Docker 网络检测（只读，仅用于显示）
    docker_network_name = fields.Char(string='Docker Network', readonly=True)
    docker_network_mode = fields.Char(string='Network Mode', readonly=True)
    docker_network_detected = fields.Boolean(string='Docker Network Detected', readonly=True)
    docker_socket_available = fields.Boolean(string='Docker Socket Available', readonly=True)

    # Connection mode selection
    connection_mode = fields.Selection([
        ('docker', 'Docker Network (Recommended)'),
        ('direct_ip', 'Direct IP Connection'),
    ], string='Connection Mode', default='docker', required=True)

    # Custom database host for direct IP mode (user-editable)
    custom_db_host = fields.Char(string='Database Host IP', default='192.168.1.100')

    # Docker network name (user can edit)
    docker_network_input = fields.Char(string='Docker Network Name', help='Enter a Docker network name. Run `docker network ls` to inspect available networks.')

    deploy_command = fields.Text(string='Deploy Command', readonly=True)
    deploy_status = fields.Text(string='Status', readonly=True)

    # ── 第 3 步：连接测试 ───────────────────────────────────────

    foggy_url = fields.Char(string='Foggy MCP URL', default='http://localhost:7108')
    connection_status = fields.Text(string='Connection Status', readonly=True)

    # ── 第 4 步：数据源 ───────────────────────────────────────────

    db_host = fields.Char(string='Database Host')
    db_port = fields.Char(string='Database Port')
    db_user = fields.Char(string='Database User')
    db_password = fields.Char(string='Database Password')
    db_name = fields.Char(string='Database Name')
    datasource_status = fields.Text(string='Data Source Status', readonly=True)

    # ── 第 5 步：闭包表 ────────────────────────────────────────

    closure_status = fields.Text(string='Closure Table Status', readonly=True)

    # ══════════════════════════════════════════════════════════════════
    # Defaults
    # ══════════════════════════════════════════════════════════════════

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        config = tools.config

        # Detect Docker environment
        docker_info = _get_docker_network_info()

        # Set smart default URL based on Docker environment
        if docker_info['in_docker'] and docker_info['network_name']:
            # Same Docker network - use container name
            default_url = 'http://foggy-mcp:8080'
        elif docker_info['in_docker']:
            # In Docker but no custom network
            default_url = 'http://host.docker.internal:8080'
        else:
            # Not in Docker - use localhost
            default_url = 'http://localhost:8080'

        # Determine if we have real network info (from Docker socket)
        # or just inferred from db_host
        socket_available = os.path.exists('/var/run/docker.sock')
        real_network_name = docker_info.get('network_name') if socket_available else None

        # 读取当前引擎模式
        ICP = self.env['ir.config_parameter'].sudo()
        engine_mode = ICP.get_param('foggy_mcp.engine_mode', 'embedded')

        res.update({
            'engine_mode': engine_mode,
            'db_host': config.get('db_host') or 'localhost',
            'db_port': str(config.get('db_port') or '5432'),
            'db_user': config.get('db_user') or 'odoo',
            'db_password': config.get('db_password') or '',
            'db_name': self.env.cr.dbname,
            'auth_token': 'foggy_' + secrets.token_hex(16),
            'foggy_url': default_url,
            'docker_network_name': real_network_name,
            'docker_network_mode': docker_info.get('network_mode'),
            'docker_network_detected': docker_info['in_docker'] and bool(docker_info.get('network_name')),
            'docker_socket_available': socket_available,
            'docker_network_input': real_network_name or '',
            'custom_db_host': '192.168.1.100',
        })
        return res

    # ══════════════════════════════════════════════════════════════════
    # Navigation
    # ══════════════════════════════════════════════════════════════════

    _GATEWAY_STEPS = ['welcome', 'deploy', 'connection', 'datasource', 'closure', 'done']
    _EMBEDDED_STEPS = ['welcome', 'closure', 'done']

    def _get_steps(self):
        """Return the step list for the selected engine mode."""
        if self.engine_mode == 'embedded':
            return self._EMBEDDED_STEPS
        return self._GATEWAY_STEPS

    def action_next(self):
        """Advance to next step."""
        self.ensure_one()

        # welcome 步骤切换模式时保存到 Settings
        if self.state == 'welcome':
            ICP = self.env['ir.config_parameter'].sudo()
            old_mode = ICP.get_param('foggy_mcp.engine_mode', 'embedded')
            if old_mode != self.engine_mode:
                ICP.set_param('foggy_mcp.engine_mode', self.engine_mode)
                _logger.info("Wizard switched engine mode: %s -> %s", old_mode, self.engine_mode)
                # 重置单例
                from ..controllers.mcp_controller import _reset_singletons
                _reset_singletons()

            # 内嵌模式：测试引擎可用性
            if self.engine_mode == 'embedded':
                self._test_embedded_engine()

        steps = self._get_steps()
        idx = steps.index(self.state)
        if idx < len(steps) - 1:
            next_state = steps[idx + 1]
            vals = {'state': next_state}

            if next_state == 'deploy':
                vals['deploy_command'] = self._generate_deploy_command()

            self.write(vals)
        return self._reopen()

    def action_prev(self):
        """Go back to previous step."""
        self.ensure_one()
        steps = self._get_steps()
        idx = steps.index(self.state)
        if idx > 0:
            self.write({'state': steps[idx - 1]})
        return self._reopen()

    def _test_embedded_engine(self):
        """Test whether the embedded engine is available."""
        try:
            from ..services.engine_factory import create_backend
            backend = create_backend(self.env)
            ok = backend.ping()
            if ok:
                self.write({
                    'embedded_status': _("Embedded engine started successfully.\n\nMode: %(mode)s") % {
                        'mode': backend.get_mode(),
                    }
                })
            else:
                self.write({
                    'embedded_status': _("Embedded engine initialized, but the ping check failed.")
                })
        except Exception as e:
            self.write({
                'embedded_status': _("Embedded engine unavailable: %(error)s") % {
                    'error': e,
                }
            })

    def _reopen(self):
        """Reopen the wizard on the same record."""
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    # ══════════════════════════════════════════════════════════════════
    # Deploy command generation
    # ══════════════════════════════════════════════════════════════════

    def _generate_deploy_command(self):
        """Generate one-line docker run command for current platform."""
        self.ensure_one()

        # Detect OS for cross-platform support
        system = platform.system()

        # Detect Docker environment and network
        docker_info = _get_docker_network_info()

        # Build environment variables - no database config, uses DataSource API
        env_vars = [
            f'-e SPRING_PROFILES_ACTIVE=lite,odoo',
            f'-e FOGGY_AUTH_TOKEN={self.auth_token}',
        ]

        # Network options and DB host based on connection mode
        network_opts = []
        port_mapping = f"-p {self.foggy_port}:8080"
        network_comment = ""
        db_host_env = ""

        if self.connection_mode == 'docker':
            # Docker network mode
            network_name = self.docker_network_input or docker_info.get('network_name')

            if network_name:
                network_opts.append(f"--network {network_name}")
            elif docker_info['in_docker']:
                # No network name available - use placeholder
                network_opts.append("--network <NETWORK_NAME>")
                network_comment = "\n# ⚠️ Replace <NETWORK_NAME> with your Docker network name."
                network_comment += "\n#    Run: docker network ls"
                if docker_info.get('container_id'):
                    network_comment += f"\n#    Or join Odoo's network: --network container:{docker_info['container_id']}"
            elif docker_info['network_mode'] == 'host':
                # Host network mode - no port mapping needed
                network_opts.append("--network host")
                port_mapping = ""
        elif self.connection_mode == 'direct_ip':
            # Direct IP connection mode
            # Use host.docker.internal to access host machine's database
            db_host_env = f'-e DB_HOST={self.custom_db_host}'
            if system == 'Linux':
                network_opts.append('--add-host=host.docker.internal:host-gateway')

        cmd = (
            f"docker run -d \\\n"
            f"  --name foggy-mcp \\\n"
        )

        if port_mapping:
            cmd += f"  {port_mapping} \\\n"

        cmd += f"  {' '.join(env_vars)} \\\n"

        if db_host_env:
            cmd += f"  {db_host_env} \\\n"

        if network_opts:
            cmd += f"  {' '.join(network_opts)} \\\n"

        cmd += (
            f"  --restart unless-stopped \\\n"
            f"  {FOGGY_DOCKER_IMAGE}"
        )

        if network_comment:
            cmd += network_comment

        return cmd

    def _get_foggy_url_hint(self):
        """Get hint for Foggy MCP URL based on Docker environment."""
        docker_info = _get_docker_network_info()

        if docker_info['in_docker'] and docker_info['network_name']:
            # Same Docker network - use container name
            return "http://foggy-mcp:8080"
        elif docker_info['in_docker']:
            # In Docker but no custom network - use host.docker.internal
            return "http://host.docker.internal:8080"
        else:
            # Not in Docker - use localhost
            return "http://localhost:8080"

    def action_regenerate_command(self):
        """Regenerate deploy command after user changes config."""
        self.ensure_one()
        self.write({'deploy_command': self._generate_deploy_command()})
        return self._reopen()

    # ══════════════════════════════════════════════════════════════════
    # Step 3: Connection test
    # ══════════════════════════════════════════════════════════════════

    def action_test_connection(self):
        """Test connectivity to Foggy MCP Server (without saving)."""
        self.ensure_one()
        url = self.foggy_url or 'http://localhost:7108'

        status_msg = ""
        is_success = False

        try:
            import requests
            r = requests.get(f'{url.rstrip("/")}/actuator/health', timeout=5)
            if r.status_code == 200:
                is_success = True
                status_msg = _(
                    "Connected to the Foggy MCP Server successfully.\n\n"
                    "URL: %(url)s\n"
                    "Auth token: %(token)s\n\n"
                    "Response: %(response)s"
                ) % {
                    'url': url,
                    'token': self.auth_token,
                    'response': r.text[:200],
                }
            else:
                status_msg = _(
                    "Server returned HTTP %(status)s\n\n%(response)s"
                ) % {
                    'status': r.status_code,
                    'response': r.text[:200],
                }
        except ImportError:
            status_msg = _("Missing the `requests` package.\n\nInstall it with: pip install requests")
        except Exception as e:
            status_msg = _("Unable to connect to %(url)s\n\nError: %(error)s") % {
                'url': url,
                'error': e,
            }

        self.write({'connection_status': status_msg})

        # If test succeeded, save configuration
        if is_success:
            ICP = self.env['ir.config_parameter'].sudo()
            ICP.set_param('foggy_mcp.server_url', url)
            ICP.set_param('foggy_mcp.auth_token', self.auth_token)

        return self._reopen()

    def action_save_connection(self):
        """Manually save connection configuration."""
        self.ensure_one()
        url = self.foggy_url or 'http://localhost:7108'

        ICP = self.env['ir.config_parameter'].sudo()
        ICP.set_param('foggy_mcp.server_url', url)
        ICP.set_param('foggy_mcp.auth_token', self.auth_token)

        # Show confirmation message
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('Configuration saved'),
                'message': _('The Foggy MCP server URL and auth token have been saved.'),
                'type': 'success',
                'sticky': False,
            }
        }

    # ══════════════════════════════════════════════════════════════════
    # Step 4: Data Source Configuration
    # ══════════════════════════════════════════════════════════════════

    def action_configure_datasource(self):
        """Register Odoo database as data source in Foggy MCP Server."""
        self.ensure_one()

        url = self.foggy_url or 'http://localhost:7108'
        ICP = self.env['ir.config_parameter'].sudo()
        auth_token = ICP.get_param('foggy_mcp.auth_token', '')

        # Determine database host based on connection mode
        if self.connection_mode == 'docker':
            # Docker network mode - use PostgreSQL service name
            # In Docker network, Foggy can reach DB via service name like 'postgres'
            db_host = 'postgres'
            # Try to get actual postgres container name from Odoo config
            config = tools.config
            pg_host = config.get('db_host')
            if pg_host and pg_host not in ('localhost', '127.0.0.1', 'False', False):
                db_host = pg_host
        elif self.connection_mode == 'direct_ip':
            # Direct IP mode - use user-provided IP
            db_host = self.custom_db_host
        else:
            # Fallback to auto-detection
            docker_info = _get_docker_network_info()
            db_host = self.db_host
            if docker_info['in_docker'] and docker_info['network_name']:
                if db_host in ('localhost', '127.0.0.1'):
                    config = tools.config
                    pg_host = config.get('db_host')
                    if pg_host and pg_host not in ('localhost', '127.0.0.1', 'False', False):
                        db_host = pg_host
                    else:
                        db_host = 'postgres'
            elif db_host in ('localhost', '127.0.0.1'):
                db_host = 'host.docker.internal'

        try:
            import requests

            # Call DataSource API
            r = requests.post(
                f'{url.rstrip("/")}/api/v1/datasource',
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {auth_token}',
                },
                json={
                    'name': 'odoo',
                    'host': db_host,
                    'port': int(self.db_port),
                    'database': self.db_name,
                    'username': self.db_user,
                    'password': self.db_password,
                    'driver': 'postgresql',
                },
                timeout=10,
            )

            if r.status_code == 200:
                # Test the connection
                r_test = requests.get(
                    f'{url.rstrip("/")}/api/v1/datasource/odoo/test',
                    headers={'Authorization': f'Bearer {auth_token}'},
                    timeout=10,
                )

                if r_test.status_code == 200:
                    result = r_test.json()
                    if result.get('data', {}).get('success'):
                        self.write({
                            'datasource_status': _(
                                "Data source configured successfully.\n\n"
                                "Name: odoo\n"
                                "Host: %(host)s:%(port)s\n"
                                "Database: %(database)s"
                            ) % {
                                'host': db_host,
                                'port': self.db_port,
                                'database': self.db_name,
                            }
                        })
                    else:
                        self.write({
                            'datasource_status': _(
                                "Data source registered, but the connection test failed.\n\n"
                                "Error: %(error)s"
                            ) % {
                                'error': result.get('data', {}).get('message', _('unknown error')),
                            }
                        })
                else:
                    self.write({
                        'datasource_status': _(
                            "Data source registered, but the test endpoint returned HTTP %(status)s."
                        ) % {
                            'status': r_test.status_code,
                        }
                    })
            else:
                self.write({
                    'datasource_status': _(
                        "Failed to configure the data source.\n\nHTTP %(status)s\n%(response)s"
                    ) % {
                        'status': r.status_code,
                        'response': r.text[:200],
                    }
                })

        except ImportError:
            self.write({
                'datasource_status': _("Missing the `requests` package.\n\nInstall it with: pip install requests")
            })
        except Exception as e:
            self.write({
                'datasource_status': _("Error: %(error)s") % {
                    'error': e,
                }
            })

        return self._reopen()

    # ══════════════════════════════════════════════════════════════════
    # Step 5: Closure tables
    # ══════════════════════════════════════════════════════════════════

    def action_init_closure_tables(self):
        """Execute closure table SQL."""
        self.ensure_one()
        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'setup', 'sql', 'refresh_closure_tables.sql',
        )
        try:
            # Check if SQL file exists
            if not os.path.exists(sql_path):
                self.write({
                    'closure_status': _(
                        "SQL file not found: %(path)s\n\n"
                        "This step is optional and can be skipped."
                    ) % {
                        'path': sql_path,
                    }
                })
                return self._reopen()

            with open(sql_path, 'r', encoding='utf-8') as f:
                sql = f.read()

            # Execute in transaction - will rollback on error
            self.env.cr.execute(sql)
            self.env.cr.execute("SELECT refresh_all_closures()")
            self.env.cr.fetchone()

            tables = ['res_company_closure', 'hr_department_closure',
                      'hr_employee_closure', 'res_partner_closure']
            counts = []
            for table in tables:
                # Use safe SQL identifier (table names are hardcoded above)
                self.env.cr.execute(f"SELECT count(*) FROM {table}")
                count = self.env.cr.fetchone()[0]
                counts.append(f"  {table}: {count} rows")

            # Also initialize date dimension table
            dim_date_status = self._init_dim_date()

            self.write({
                'closure_status': _("Closure tables initialized successfully.\n\n")
                                  + '\n'.join(counts)
                                  + '\n\n' + dim_date_status
            })
        except Exception as e:
            # Log the error for debugging, but show user-friendly message
            _logger.warning("Closure table initialization failed: %s", e, exc_info=True)
            self.write({
                'closure_status': _(
                    "Error: %(error)s\n\n"
                    "You can skip this step and initialize closure tables later.\n"
                    "Hierarchical queries (child_of / parent_of) can still fall back to Odoo's native parent_path."
                ) % {
                    'error': e,
                }
            })

        return self._reopen()

    def _init_dim_date(self):
        """Initialize date dimension table. Returns status string."""
        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'setup', 'sql', 'create_dim_date.sql',
        )
        try:
            if not os.path.exists(sql_path):
                return _("Date dimension SQL file not found (optional).")

            with open(sql_path, 'r', encoding='utf-8') as f:
                self.env.cr.execute(f.read())
            self.env.cr.execute("SELECT create_or_refresh_dim_date(2020, 2035)")
            row_count = self.env.cr.fetchone()[0]
            return _("Date dimension initialized successfully: dim_date %(count)s rows (2020-2035)") % {
                'count': row_count,
            }
        except Exception as e:
            _logger.warning("Date dimension initialization failed: %s", e)
            return _("Date dimension initialization failed: %(error)s (optional; core functionality is unaffected)") % {
                'error': e,
            }

    def action_skip_closure(self):
        """Skip closure table initialization."""
        self.ensure_one()
        return self.action_next()

    # ══════════════════════════════════════════════════════════════════
    # Step 6: Finish
    # ══════════════════════════════════════════════════════════════════

    def action_finish(self):
        """Close wizard and redirect to API Key creation."""
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'foggy.api.key',
            'view_mode': 'form',
            'target': 'current',
            'context': {'default_name': 'My API Key'},
        }
