# -*- coding: utf-8 -*-
"""Odoo 员工模型（hr_employee）"""
from ._helpers import (
    odoo_model, dim, dim_time, measure_count,
    fk_join, prop,
)


def create_hr_employee_model():
    return odoo_model(
        name='OdooHrEmployeeModel',
        table='hr_employee',
        alias='员工',
        dimensions={
            'id': dim('id', 'id', '员工ID'),
            'name': dim('name', 'name', '姓名'),
            'jobTitle': dim('jobTitle', 'job_title', '职位'),
            'workEmail': dim('workEmail', 'work_email', '工作邮箱'),
            'workPhone': dim('workPhone', 'work_phone', '工作电话'),
            'active': dim('active', 'active', '在职'),
            'gender': dim('gender', 'gender', '性别'),
            'marital': dim('marital', 'marital', '婚姻状况'),
            'employeeType': dim('employeeType', 'employee_type', '员工类型'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'employeeCount': measure_count('employeeCount', 'id', '员工数'),
        },
        dimension_joins=[
            fk_join('department', 'hr_department', 'department_id', caption='部门',
                    caption_column='complete_name'),
            fk_join('job', 'hr_job', 'job_id', caption='岗位'),
            fk_join('company', 'res_company', 'company_id', caption='公司'),
            fk_join('parent', 'hr_employee', 'parent_id', caption='上级'),
            fk_join('user', 'res_users', 'user_id', caption='关联用户',
                    caption_column='login'),
        ],
    )
