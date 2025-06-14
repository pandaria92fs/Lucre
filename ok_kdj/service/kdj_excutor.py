import os

from datetime import datetime
from ok_kdj.service.calac_V4  import DirectKDJCalculator
from ok_kdj.service.kdj_pusher import KDJPushBot# 假设你的KDJ类保存在这个文件中
# 在 kdj_excutor.py 中，不使用相对导入，改用绝对路径导入
import sys
import os
# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)
import time
import os
from datetime import datetime
# 现在可以直接导入
from ok_kdj.proxy import okproxy

# 获取项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
def process_all_latest_files(root_dir="service/kline_data_folder"):
    """
    处理所有交易对的最新CSV文件，计算KDJ指标

    参数:
        root_dir: 包含交易对文件夹的根目录
    """
    # 1. 首先找到所有最新的CSV文件
    latest_files = find_latest_csv_files(root_dir)

    if not latest_files:
        print("❌ 没有找到任何交易对的最新CSV文件")
        return

    print(f"🔍 找到 {len(latest_files)} 个交易对的最新CSV文件:")
    for symbol, filepath in latest_files.items():
        print(f"  {symbol}: {filepath}")

    # 2. 初始化KDJ计算器
    kdj_calculator = DirectKDJCalculator(rsv_period=9)

    # 3. 处理每个文件
    results = {}
    for symbol, filepath in latest_files.items():
        print(f"\n{'=' * 60}")
        print(f"📊 开始处理 {symbol} 数据...")

        try:
            # 处理CSV文件 (不提供目标值，使用默认方法)
            result = kdj_calculator.process_csv_file(filepath, target_values=None)

            if result is not None:
                results[symbol] = {
                    'filepath': filepath,
                    'result': result,
                    'latest_kdj': result[['K', 'D', 'J']].iloc[-1].to_dict()
                }
                print(f"✅ {symbol} 处理完成!")
            else:
                print(f"❌ {symbol} 处理失败")

        except Exception as e:
            print(f"❌ 处理 {symbol} 时出错: {str(e)}")

    # 4. 汇总结果
    if results:
        print("\n🎉 所有文件处理完成! 汇总结果:")
        print("=" * 60)
        for symbol, data in results.items():
            k, d, j = data['latest_kdj']['K'], data['latest_kdj']['D'], data['latest_kdj']['J']
            print(f"{symbol:<6} | K: {k:7.4f} | D: {d:7.4f} | J: {j:8.4f}")
    else:
        print("\n❌ 没有成功处理任何文件")


import os
from datetime import datetime


def find_latest_direct_kdj_csv_files(root_dir):
    """
    查找每个交易对文件夹中最新的 _direct_kdj.csv 文件（精确到小时）

    参数:
        root_dir: 根目录，例如 'service/kline_data_folder'

    返回:
        字典: {交易对名称: 最新文件路径}
    """
    latest_kdj_files = {}

    for symbol in os.listdir(root_dir):
        symbol_path = os.path.join(root_dir, symbol)

        if os.path.isdir(symbol_path):
            latest_file = None
            latest_datetime = None  # 改为存储datetime对象

            for filename in os.listdir(symbol_path):
                if filename.endswith('_direct_kdj.csv'):
                    try:
                        # 提取日期和时间部分（如 20250614_14）
                        date_part, time_part = filename.split('_')[:2]
                        time_part = time_part.split('.')[0]  # 移除可能的扩展名

                        # 解析完整时间（年月日+小时）
                        file_dt = datetime.strptime(
                            f"{date_part}{time_part}",
                            "%Y%m%d%H"
                        )

                        # 比较时间（精确到小时）
                        if latest_datetime is None or file_dt > latest_datetime:
                            latest_datetime = file_dt
                            latest_file = os.path.join(symbol_path, filename)

                    except (ValueError, IndexError) as e:
                        print(f"跳过格式错误的文件名: {filename} ({str(e)})")
                        continue

            if latest_file:
                latest_kdj_files[symbol] = latest_file
                print(f"找到 {symbol} 的最新文件: {latest_file} (时间: {latest_datetime})")

    return latest_kdj_files
def find_latest_csv_files(root_dir):
    """
    在给定的根目录下查找每个交易对文件夹中最新的CSV文件

    参数:
        root_dir: 要搜索的根目录路径

    返回:
        字典: {交易对名称: 最新文件的完整路径}
    """
    latest_files = {}

    # 遍历根目录下的所有交易对文件夹
    for symbol in os.listdir(root_dir):
        symbol_path = os.path.join(root_dir, symbol)

        if os.path.isdir(symbol_path):
            latest_file = None
            latest_date = None

            # 遍历该交易对文件夹下的所有CSV文件
            for filename in os.listdir(symbol_path):
                if filename.endswith('.csv'):
                    try:
                        # 从文件名解析日期时间 (格式: 20250614_11.csv)
                        date_str, time_str = filename.split('_')
                        time_str = time_str.split('.')[0]  # 移除.csv
                        file_date = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H")

                        # 比较日期
                        if latest_date is None or file_date > latest_date:
                            latest_date = file_date
                            latest_file = os.path.join(symbol_path, filename)
                    except ValueError:
                        # 如果文件名不符合预期格式，跳过
                        continue

            if latest_file:
                latest_files[symbol] = latest_file

    return latest_files

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


if __name__ == "__main__":

    # 主程序入口
    print("=" * 60)
    print("📈 KDJ指标批量计算工具")
    print("=" * 60)

    # 记录总开始时间
    total_start_time = time.time()
    program_start_time = datetime.now()
    print(f"🕐 程序启动时间: {program_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 第一步：获取K线数据
    print("\n🚀 步骤1: 开始并发获取K线数据...")
    kline_start_time = time.time()

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

        print(f"✅ K线数据获取完成!")
        print(f"📊 获取用时: {format_duration(kline_duration)}")
        print(f"💾 保存用时: {format_duration(save_duration)}")
        print(f"🎯 获取到 {len(results)} 个交易对的数据")

        # 统计成功失败数量
        success_count = sum(1 for r in results.values() if r['success'])
        failed_count = len(results) - success_count
        print(f"📈 成功: {success_count} 个, 失败: {failed_count} 个")

    else:
        print("❌ 没有获取到任何K线数据")
        print(f"⏱️  获取用时: {format_duration(kline_duration)}")

    # 第二步：处理KDJ计算
    print("\n🔄 步骤2: 开始KDJ指标计算...")
    processing_start_time = time.time()

    # 指定数据目录 (可以根据需要修改)
    data_directory = os.path.join(project_root, 'service', 'kline_data_folder')

    # 处理所有最新文件
    process_all_latest_files(data_directory)

    processing_end_time = time.time()
    processing_duration = processing_end_time - processing_start_time

    print(f"✅ KDJ指标计算完成!")
    print(f"🧮 计算用时: {format_duration(processing_duration)}")

    # 第三步：推送处理
    print("\n📤 步骤3: 开始数据推送处理...")
    push_start_time = time.time()

    print("\n📂 最新 _direct_kdj.csv 文件列表:")
    latest_direct_kdj_files = find_latest_direct_kdj_csv_files(data_directory)
    bot = KDJPushBot()

    processed_files = 0
    for symbol, path in latest_direct_kdj_files.items():
        print(f"📄 处理文件: {path}")
        file_start_time = time.time()

        try:
            bot.process_file(path)
            file_end_time = time.time()
            file_duration = file_end_time - file_start_time
            processed_files += 1
            print(f"✅ {symbol} 处理完成 (用时: {format_duration(file_duration)})")
        except Exception as e:
            file_end_time = time.time()
            file_duration = file_end_time - file_start_time
            print(f"❌ {symbol} 处理失败: {str(e)} (用时: {format_duration(file_duration)})")

    push_end_time = time.time()
    push_duration = push_end_time - push_start_time

    print(f"✅ 数据推送处理完成!")
    print(f"📤 推送用时: {format_duration(push_duration)}")
    print(f"📊 处理文件数: {processed_files}/{len(latest_direct_kdj_files)}")

    # 总结报告
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    program_end_time = datetime.now()

    print("\n" + "=" * 60)
    print("📋 执行总结报告")
    print("=" * 60)
    print(f"🕐 开始时间: {program_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🕐 结束时间: {program_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  总耗时: {format_duration(total_duration)}")
    print("\n📊 各步骤耗时详情:")
    print(f"   📈 K线数据获取: {format_duration(kline_duration)} ({kline_duration / total_duration * 100:.1f}%)")
    if 'save_duration' in locals():
        print(f"   💾 数据保存: {format_duration(save_duration)} ({save_duration / total_duration * 100:.1f}%)")
    print(
        f"   🧮 KDJ指标计算: {format_duration(processing_duration)} ({processing_duration / total_duration * 100:.1f}%)")
    print(f"   📤 数据推送处理: {format_duration(push_duration)} ({push_duration / total_duration * 100:.1f}%)")

    if results:
        avg_time_per_symbol = kline_duration / len(results)
        print(f"\n⚡ 性能指标:")
        print(f"   📈 平均每个交易对获取时间: {format_duration(avg_time_per_symbol)}")
        print(f"   🚀 数据获取速度: {len(results) / kline_duration:.2f} 个/秒")

    # 如果总时间超过预期，给出提示
    if total_duration > 300:  # 5分钟
        print(f"\n⚠️  注意: 总执行时间较长 ({format_duration(total_duration)})，可能需要优化")
    elif total_duration > 60:  # 1分钟
        print(f"\n💡 提示: 执行时间 {format_duration(total_duration)}，性能良好")
    else:
        print(f"\n🚀 优秀: 执行时间仅 {format_duration(total_duration)}，性能优异!")

    print("=" * 60)
