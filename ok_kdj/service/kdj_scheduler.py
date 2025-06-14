import time
import os
import logging
import schedule
import threading
from datetime import datetime, timedelta
import signal
import sys


def setup_logger(name="KDJ_Scheduler", log_level=logging.INFO):
    """
    设置日志配置

    Args:
        name (str): 日志器名称
        log_level: 日志级别

    Returns:
        logging.Logger: 配置好的日志器
    """
    # 创建日志器
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = 'kdj_scheduler.log'
    log_filepath = os.path.join(log_dir, log_filename)

    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def format_duration(seconds):
    """
    格式化时间显示

    Args:
        seconds (float): 秒数

    Returns:
        str: 格式化的时间字符串
    """
    if seconds < 60:
        return f"{seconds:.2f}秒"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}分{remaining_seconds:.1f}秒"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        remaining_seconds = seconds % 60
        return f"{hours}小时{minutes}分{remaining_seconds:.1f}秒"


class KDJScheduler:
    def __init__(self):
        self.logger = setup_logger()
        self.is_running = False
        self.task_count = 0
        self.last_run_time = None
        self.scheduler_start_time = datetime.now()

        # 信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """处理退出信号"""
        self.logger.info(f"🛑 接收到退出信号 {signum}，正在安全关闭...")
        self.stop()
        sys.exit(0)

    def kdj_calculation_task(self):
        """
        KDJ计算主任务
        """
        task_start_time = time.time()
        self.task_count += 1
        self.last_run_time = datetime.now()

        # 创建任务专用日志器
        task_logger = setup_logger(f"KDJ_Task_{self.task_count}")

        task_logger.info("=" * 60)
        task_logger.info(f"📈 KDJ指标批量计算工具 - 第 {self.task_count} 次执行")
        task_logger.info("=" * 60)

        # 记录总开始时间
        total_start_time = time.time()
        program_start_time = datetime.now()
        task_logger.info(f"🕐 任务启动时间: {program_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 第一步：获取K线数据
        task_logger.info("🚀 步骤1: 开始并发获取K线数据...")
        kline_start_time = time.time()

        try:
            import okproxy  # 根据你的实际导入路径调整
            fetcher = okproxy.OKXKlineFetcher()

            # 使用异步并发模式，每秒5个请求
            results = fetcher.fetch_all_klines(bar="1H", limit=30, use_async=True)

            kline_end_time = time.time()
            kline_duration = kline_end_time - kline_start_time

            if results:
                save_start_time = time.time()
                fetcher.save_klines_to_csv(results)
                save_end_time = time.time()
                save_duration = save_end_time - save_start_time

                task_logger.info("✅ K线数据获取完成!")
                task_logger.info(f"📊 获取用时: {format_duration(kline_duration)}")
                task_logger.info(f"💾 保存用时: {format_duration(save_duration)}")
                task_logger.info(f"🎯 获取到 {len(results)} 个交易对的数据")

                # 统计成功失败数量
                success_count = sum(1 for r in results.values() if r['success'])
                failed_count = len(results) - success_count
                task_logger.info(f"📈 成功: {success_count} 个, 失败: {failed_count} 个")

                if failed_count > 0:
                    task_logger.warning(f"⚠️ 有 {failed_count} 个交易对获取失败")

            else:
                task_logger.error("❌ 没有获取到任何K线数据")
                task_logger.info(f"⏱️ 获取用时: {format_duration(kline_duration)}")

        except Exception as e:
            kline_end_time = time.time()
            kline_duration = kline_end_time - kline_start_time
            task_logger.error(f"❌ K线数据获取过程中发生异常: {str(e)}")
            task_logger.info(f"⏱️ 异常前用时: {format_duration(kline_duration)}")
            results = None

        # 第二步：处理KDJ计算
        task_logger.info("🔄 步骤2: 开始KDJ指标计算...")
        processing_start_time = time.time()

        try:
            # 指定数据目录 (根据你的实际项目结构调整)
            project_root = os.path.dirname(os.path.dirname(__file__))  # 根据实际情况调整
            data_directory = os.path.join(project_root, 'service', 'kline_data_folder')
            task_logger.debug(f"数据目录: {data_directory}")

            # 这里需要根据你的实际函数名调整
            # process_all_latest_files(data_directory)

            processing_end_time = time.time()
            processing_duration = processing_end_time - processing_start_time

            task_logger.info("✅ KDJ指标计算完成!")
            task_logger.info(f"🧮 计算用时: {format_duration(processing_duration)}")

        except Exception as e:
            processing_end_time = time.time()
            processing_duration = processing_end_time - processing_start_time
            task_logger.error(f"❌ KDJ指标计算过程中发生异常: {str(e)}")
            task_logger.info(f"⏱️ 异常前用时: {format_duration(processing_duration)}")

        # 第三步：推送处理
        task_logger.info("📤 步骤3: 开始数据推送处理...")
        push_start_time = time.time()

        try:
            task_logger.info("📂 查找最新 _direct_kdj.csv 文件...")
            # latest_direct_kdj_files = find_latest_direct_kdj_csv_files(data_directory)
            # task_logger.info(f"找到 {len(latest_direct_kdj_files)} 个KDJ文件")

            # bot = KDJPushBot()

            processed_files = 0
            failed_files = 0

            # for symbol, path in latest_direct_kdj_files.items():
            #     ... 处理逻辑

            push_end_time = time.time()
            push_duration = push_end_time - push_start_time

            task_logger.info("✅ 数据推送处理完成!")
            task_logger.info(f"📤 推送用时: {format_duration(push_duration)}")

        except Exception as e:
            push_end_time = time.time()
            push_duration = push_end_time - push_start_time
            task_logger.error(f"❌ 数据推送处理过程中发生异常: {str(e)}")
            task_logger.info(f"⏱️ 异常前用时: {format_duration(push_duration)}")

        # 总结报告
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        program_end_time = datetime.now()

        task_logger.info("=" * 60)
        task_logger.info("📋 执行总结报告")
        task_logger.info("=" * 60)
        task_logger.info(f"🕐 开始时间: {program_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.info(f"🕐 结束时间: {program_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.info(f"⏱️ 总耗时: {format_duration(total_duration)}")

        # 更新调度器统计
        self.logger.info(f"✅ 第 {self.task_count} 次任务执行完成，用时: {format_duration(total_duration)}")

        # 计算下次执行时间
        next_run = self.get_next_run_time()
        self.logger.info(f"⏰ 下次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

    def get_next_run_time(self):
        """计算下次执行时间（下个整点的30秒）"""
        now = datetime.now()
        next_hour = now.replace(minute=0, second=30, microsecond=0) + timedelta(hours=1)
        return next_hour

    def wait_for_next_run(self):
        """等待到下次执行时间"""
        next_run = self.get_next_run_time()
        now = datetime.now()
        wait_seconds = (next_run - now).total_seconds()

        if wait_seconds > 0:
            self.logger.info(f"⏳ 等待 {format_duration(wait_seconds)} 到下次执行时间: {next_run.strftime('%H:%M:%S')}")
            time.sleep(wait_seconds)

    def run_once(self):
        """立即运行一次任务"""
        self.logger.info("🚀 手动启动任务执行...")
        self.kdj_calculation_task()

    def start_scheduler(self):
        """启动定时调度器"""
        self.is_running = True
        self.logger.info("🔄 KDJ定时调度器启动")
        self.logger.info(f"📅 调度器启动时间: {self.scheduler_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("⏰ 任务将在每小时的整点过30秒执行")

        # 设置定时任务：每小时的30秒执行
        for hour in range(24):
            schedule.every().day.at(f"{hour:02d}:00:30").do(self.kdj_calculation_task)

        # 显示下次执行时间
        next_run = self.get_next_run_time()
        self.logger.info(f"⏰ 下次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("🛑 接收到键盘中断，正在停止调度器...")
            self.stop()

    def start_simple_scheduler(self):
        """启动简单的定时调度器（不依赖schedule库）"""
        self.is_running = True
        self.logger.info("🔄 KDJ简单定时调度器启动")
        self.logger.info(f"📅 调度器启动时间: {self.scheduler_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("⏰ 任务将在每小时的整点过30秒执行")

        try:
            while self.is_running:
                now = datetime.now()

                # 检查是否到了执行时间（每小时的30秒）
                if now.minute == 0 and now.second == 30:
                    self.kdj_calculation_task()
                    # 等待到下一分钟，避免重复执行
                    time.sleep(60)
                else:
                    # 计算到下次执行的等待时间
                    next_run = self.get_next_run_time()
                    wait_seconds = min((next_run - now).total_seconds(), 60)

                    if wait_seconds > 0:
                        time.sleep(min(wait_seconds, 1))  # 最多等待1秒，保持响应性

        except KeyboardInterrupt:
            self.logger.info("🛑 接收到键盘中断，正在停止调度器...")
            self.stop()

    def stop(self):
        """停止调度器"""
        self.is_running = False
        total_runtime = datetime.now() - self.scheduler_start_time
        self.logger.info("🛑 KDJ调度器已停止")
        self.logger.info(f"📊 总运行时间: {format_duration(total_runtime.total_seconds())}")
        self.logger.info(f"📈 总执行任务数: {self.task_count}")
        if self.last_run_time:
            self.logger.info(f"🕐 最后执行时间: {self.last_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

    def status(self):
        """显示调度器状态"""
        now = datetime.now()
        runtime = now - self.scheduler_start_time
        next_run = self.get_next_run_time()

        self.logger.info("📊 调度器状态信息:")
        self.logger.info(f"   🔄 运行状态: {'运行中' if self.is_running else '已停止'}")
        self.logger.info(f"   📅 启动时间: {self.scheduler_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"   ⏱️ 运行时长: {format_duration(runtime.total_seconds())}")
        self.logger.info(f"   📈 执行次数: {self.task_count}")
        if self.last_run_time:
            self.logger.info(f"   🕐 最后执行: {self.last_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"   ⏰ 下次执行: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """主函数"""
    scheduler = KDJScheduler()

    # 命令行参数处理
    import argparse
    parser = argparse.ArgumentParser(description='KDJ定时任务调度器')
    parser.add_argument('--run-once', action='store_true', help='立即执行一次任务后退出')
    parser.add_argument('--status', action='store_true', help='显示调度器状态')
    parser.add_argument('--simple', action='store_true', help='使用简单调度器（不依赖schedule库）')

    args = parser.parse_args()

    if args.run_once:
        scheduler.run_once()
    elif args.status:
        scheduler.status()
    elif args.simple:
        scheduler.start_simple_scheduler()
    else:
        scheduler.start_scheduler()


if __name__ == "__main__":
    main()