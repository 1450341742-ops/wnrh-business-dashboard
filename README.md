# 万宁睿和经营分析驾驶舱 Enterprise V2.0

企业版 Streamlit 经营分析系统，覆盖项目毛利、人力成本、差旅成本、现金流、回款周期、报价模型、客户利润贡献、人员产能利用率、项目盈亏预警。

## 已包含功能

- 账号登录与角色权限控制
- Excel / CSV 一键导入
- 排班系统数据导入对接
- 财务差旅自动识别与归集
- 老板经营驾驶舱
- 项目经营分析
- 现金流与回款周期分析
- 报价测算模型
- 客户利润贡献分析
- 人员产能利用率分析
- 项目盈亏与经营预警中心
- 老板月报 Word 自动生成
- 全部数据 ZIP 导出

## 默认账号

用户名：`admin`  
密码：`admin123`

上线后请进入【系统设置】修改管理员密码。

## 本地运行

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Streamlit Cloud 部署

1. 打开 Streamlit Cloud
2. 选择本仓库 `wnrh-business-dashboard`
3. Main file path 填写：`app.py`
4. 点击 Deploy

## 数据说明

系统首次运行会自动生成 `data/business_dashboard.db` SQLite 数据库。  
CSV / Excel 导入模板见 `templates/` 文件夹。

## 建议内部使用流程

1. 财务维护合同、回款、人员成本、差旅数据
2. 项目经理导入排班系统数据
3. 商务使用报价模型测算报价底线
4. 管理层查看项目毛利、现金流、客户贡献和人员产能
5. 老板每月导出经营月报
