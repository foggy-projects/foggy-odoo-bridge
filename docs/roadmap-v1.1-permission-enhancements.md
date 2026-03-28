# v1.1 规划 — 权限配置可视化增强

> 创建日期: 2026-03-28
> 状态: 待排期
> 依赖: v1.0 发版完成

## 背景

v1.0 实现了完整的三层权限体系（模型级访问 / 工具级过滤 / 行级过滤），
但配置和诊断依赖 Odoo 原生界面或日志排查，对管理员不够友好。
v1.0 已在 `permission_bridge.py` 中加入了字段映射完整性的 fail-closed 检查
（`PermissionFieldMappingError`），但未提供可视化配置和映射状态面板。

---

## 阶段二功能清单

### 1. 权限字段映射状态面板

**位置**: Settings → Foggy MCP → 新增"权限映射状态"区域

**功能**:
- 扫描所有 QM 模型对应的 Odoo 模型的 `ir.rule` 记录
- 提取每条规则引用的字段
- 对比 `FieldMappingRegistry` + `DIRECT_FIELD_MAP` 检查映射覆盖率
- 以表格展示映射状态

**UI 设计**:

```
┌──────────────────────────────────────────────────────────────┐
│  权限字段映射状态                                    [刷新映射]  │
├──────────────┬──────────────┬───────────────┬───────────────┤
│ Odoo 模型     │ ir.rule 字段  │ QM 字段        │ 状态          │
├──────────────┼──────────────┼───────────────┼───────────────┤
│ sale.order   │ company_id   │ company$id    │ ✅ 已映射      │
│ sale.order   │ user_id      │ salesperson$id│ ✅ 已映射      │
│ sale.order   │ team_id      │ salesTeam$id  │ ✅ 已映射      │
│ sale.order   │ website_id   │ —             │ ❌ 未映射      │
│ res.partner  │ company_id   │ company$id    │ ✅ 已映射      │
│ res.partner  │ parent_id    │ parent$id     │ ✅ 已映射      │
│ res.partner  │ partner_share│ partnerShare  │ ✅ 已映射      │
└──────────────┴──────────────┴───────────────┴───────────────┘
  ⚠️ 存在 1 个未映射字段，相关模型查询将被拒绝（fail-closed）
```

**实现要点**:
- 新增 transient model: `foggy.field.mapping.status`
- 在 Settings 页面加一个 `One2many` 字段展示扫描结果
- "刷新映射"按钮触发扫描（调用 `compute_mapping_status()` 方法）
- 扫描逻辑复用 `permission_bridge.py` 现有的映射查找

### 2. 模型权限概览面板（只读）

**位置**: Settings → Foggy MCP → 新增"模型权限概览"区域

**功能**:
- 展示当前已注册的 9 个 Odoo → QM 模型映射
- 显示每个模型的 ir.model.access 状态（哪些组有读权限）
- 显示每个模型生效的 ir.rule 条数

**UI 设计**:

```
┌────────────────────────────────────────────────────────────────┐
│  模型权限概览                                                    │
├────────────────┬──────────────────────┬──────────┬─────────────┤
│ Odoo 模型       │ QM 模型               │ ACL 状态  │ ir.rule 条数 │
├────────────────┼──────────────────────┼──────────┼─────────────┤
│ sale.order     │ OdooSaleOrderQM      │ ✅ 可读   │ 3           │
│ purchase.order │ OdooPurchaseOrderQM  │ ✅ 可读   │ 2           │
│ crm.lead       │ OdooCrmLeadQM        │ ✅ 可读   │ 4           │
│ hr.employee    │ OdooHrEmployeeQM     │ ✅ 可读   │ 2           │
│ ...            │ ...                  │ ...      │ ...         │
└────────────────┴──────────────────────┴──────────┴─────────────┘
```

**注意**: 这是只读面板，不修改 ACL。ACL 管理仍通过 Odoo 原生的
Settings → Technical → Access Rights 进行。

### 3. 字段映射手动补充（可选）

**评估中**: 是否允许管理员在 UI 上手动添加 Odoo 字段 → QM 字段的映射。

**方案 A**: 纯 TM/QM 驱动（推荐）
- 管理员在 TM/QM 文件中补充缺失字段
- 映射面板自动刷新
- 不引入额外的 UI 配置层

**方案 B**: UI 配置覆盖
- 新增 `foggy.field.mapping.override` 模型
- 管理员可在 UI 上手动添加映射
- 优先级: UI 配置 > FieldMappingRegistry > DIRECT_FIELD_MAP
- 风险: 增加复杂度，可能与 TM/QM 定义不一致

**当前倾向**: 方案 A（TM/QM 是唯一权威源原则）

---

## v1.0 已完成的权限基础

| 功能 | 状态 | 说明 |
|------|------|------|
| 三层权限体系 | ✅ | 模型级 + 工具级 + 行级 |
| Fail-closed 原则 | ✅ | 异常时拒绝访问 |
| 字段映射完整性检查 | ✅ | 未映射字段 → PermissionFieldMappingError |
| Hierarchy operator 支持 | ✅ | child_of/parent_of → closure table JOIN |
| Boolean vs NULL 区分 | ✅ | 运行时字段类型内省 |
| 动态字段映射 | ✅ | FieldMappingRegistry 从引擎元数据加载 |
| API Key 认证 | ✅ | fmcp_ 前缀密钥 + Session Cookie 双模式 |

---

## 优先级排序

1. **P1 — 权限字段映射状态面板**: 直接解决"为什么查询被拒绝"的诊断问题
2. **P2 — 模型权限概览面板**: 方便管理员快速了解整体权限配置
3. **P3 — 字段映射手动补充**: 待评估，倾向 TM/QM 驱动
4. **P4 — 列级权限对接（field.groups）**: 对接 Odoo 字段级权限控制

---

### 4. 列级权限对接（field.groups）

**背景**: Odoo 原生支持通过 `field.groups` 属性控制字段可见性，例如：
```python
margin = fields.Float(groups="sale.group_sale_manager")
```
无权用户通过 ORM 访问时该字段会被过滤掉。但 Foggy 引擎直接查 SQL，
绕过了 ORM 的字段权限检查，存在列级数据泄露风险。

**实现思路**:
```
MCP 查询请求 (columns: ["name", "margin", "amountTotal"])
    ↓
检查用户对每个 QM 字段 → 反查 Odoo field → 检查 field.groups
    ↓
过滤掉无权列 → 只查 ["name", "amountTotal"]
```

**关键设计点**:
- 在 `mcp_controller._handle_tools_call()` 中，获取 payload.columns 后过滤
- 需要 QM 字段 → Odoo 字段的反向映射（FieldMappingRegistry 已有正向映射）
- 检查 `env[odoo_model]._fields[field_name].groups` 是否包含用户所属组
- 如果 columns 为空（查全部），需要主动过滤掉有 groups 限制的字段
- Fail-closed: 字段权限检查异常时移除该列（而非保留）

**风险评估**:
- 当前 9 个模型中大部分字段无 groups 限制，实际影响面较小
- 薪资模块（hr.payslip）、成本字段等未来可能需要
- 优先级低于 P1-P3，按需实现

---

## 工作量估算

| 任务 | 估算 | 说明 |
|------|------|------|
| P1 映射状态面板 | 2-3 天 | transient model + views + 扫描逻辑 |
| P2 模型权限概览 | 1-2 天 | 只读面板，复用现有 ACL 查询 |
| P3 字段映射补充 | 待评估 | 倾向 TM/QM 驱动，可能不需要 UI |
| P4 列级权限对接 | 1-2 天 | QM 字段→Odoo field.groups 反向检查 + 列过滤 |
| 文档更新 | 0.5 天 | TESTING_GUIDE + CLAUDE.md |
| 总计 | 5-8 天 | |
