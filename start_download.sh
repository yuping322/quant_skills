#!/bin/bash
# 宏观数据下载启动脚本

echo "===================================="
echo "  宏观数据批量下载程序启动"
echo "===================================="
echo "开始时间: $(date)"
echo ""

# 检查Python环境
if ! command -v python &> /dev/null; then
    echo "错误: 未找到Python，请先安装Python"
    exit 1
fi

# 创建日志目录
LOG_DIR="logs"
mkdir -p $LOG_DIR

# 启动下载程序
echo "开始下载宏观数据..."
echo "详细日志请查看: macro_download.log"
echo ""

# 运行下载程序，后台运行并记录PID
nohup python src/downloaders/macro_downloader.py > $LOG_DIR/download_$(date +%Y%m%d_%H%M%S).log 2>&1 &

DOWNLOAD_PID=$!
echo $DOWNLOAD_PID > download.pid

echo "下载程序已启动 (PID: $DOWNLOAD_PID)"
echo "可以使用以下命令查看进度:"
echo "  tail -f macro_download.log"
echo " 或者查看详细日志: tail -f $LOG_DIR/download_*.log"
echo ""
echo "要停止下载，请运行: kill $DOWNLOAD_PID"
echo "===================================="