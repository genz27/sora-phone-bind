# Sora 绑定平台

批量验证 Sora 账号手机号的 Web 服务。

## 目录结构

```
├── main.py              # 主程序
├── static/
│   ├── index.html       # 用户前端
│   └── admin.html       # 管理后台
├── data/                # 数据目录（自动创建，已忽略）
│   └── sora_bind.sqlite3
├── Dockerfile           # Docker 部署
├── requirements.txt     # Python 依赖
└── README.md
```

## 快速开始

### 本地运行

```bash
pip install -r requirements.txt
python main.py
```

访问 http://localhost:8899

### Docker 部署

```bash
docker build -t sora-bind .
docker run -d -p 8899:8899 -v ./data:/app/data sora-bind
```

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `SORA_ADMIN_PASSWORD` | 管理员密码 | `admin123` |

## 功能

- RT 转 AT（并发）
- 手机号验证（并发、轮询）
- 暂停/恢复/终止任务
- 自动移除已满手机号
- 数据持久化（SQLite）

## 注意事项

- `data/` 目录包含数据库，已在 `.gitignore` 中忽略
- 生产环境请修改默认管理员密码
