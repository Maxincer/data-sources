#!/usr/bin/env bash
# NotifierWatchdog 环境变量配置
# 供 rutask / cron 调度时 source 使用
# 敏感信息与 data_sources_pipeline 保持一致

export TAVILY_API_KEY="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"
export DEEPSEEK_API_KEY="sk-9bf4150a6cfe435a8ae66da744b17487"
export FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/7f1c49ef-6e6b-4c19-8152-8e25dfb8d688"

export SMTP_HOST="smtp.exmail.qq.com"
export SMTP_PORT="465"
export SMTP_PASSWORD="NBnDH84qZHZWFrTC"
export SENDER="robot@wendao.fund"
export RECIPIENTS="mxz@wendao.fund"
