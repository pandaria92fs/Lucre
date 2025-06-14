import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import json
import os


class OKXKlineFetcher:
    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.kline_endpoint = "/api/v5/market/candles"

        # 获取当前脚本所在目录
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        # 项目根目录
        self.project_root = os.path.dirname(self.script_dir)

    def read_swap_csv(self, csv_path=None):
        """
        读取swap.csv文件获取instId列表

        Args:
            csv_path (str): CSV文件路径，如果为None则使用默认路径

        Returns:
            list: instId列表
        """
        if csv_path is None:
            # 默认路径：script/sqqq/swap.csv
            csv_path = os.path.join(self.script_dir, "swap.csv")

        # 如果传入的是相对路径，则相对于项目根目录
        if not os.path.isabs(csv_path):
            csv_path = os.path.join(self.project_root, csv_path)

        try:
            if not os.path.exists(csv_path):
                print(f"❌ 文件不存在: {csv_path}")
                return []

            df = pd.read_csv(csv_path)
            inst_ids = df['instId'].tolist()
            print(f"📊 从 {csv_path} 读取到 {len(inst_ids)} 个交易对")
            return inst_ids
        except Exception as e:
            print(f"❌ 读取CSV文件失败: {str(e)}")
            return []

    def get_kline_data(self, inst_id, bar="1H", limit=9):
        """
        获取指定交易对的K线数据

        Args:
            inst_id (str): 交易对ID，如 "BTC-USDT-SWAP"
            bar (str): K线周期，1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M/3M/6M/1Y
            limit (int): 获取的K线数量，默认9根（9小时）

        Returns:
            dict: K线数据
        """
        url = f"{self.base_url}{self.kline_endpoint}"

        params = {
            'instId': inst_id,
            'bar': bar,
            'limit': str(limit)
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get('code') == '0':
                return {
                    'instId': inst_id,
                    'success': True,
                    'data': data.get('data', []),
                    'count': len(data.get('data', []))
                }
            else:
                return {
                    'instId': inst_id,
                    'success': False,
                    'error': data.get('msg', '未知错误'),
                    'data': []
                }

        except requests.exceptions.RequestException as e:
            return {
                'instId': inst_id,
                'success': False,
                'error': f'请求失败: {str(e)}',
                'data': []
            }

    def format_kline_data(self, kline_result):
        """
        格式化K线数据为易读格式

        Args:
            kline_result (dict): get_kline_data返回的结果

        Returns:
            list: 格式化后的K线数据
        """
        if not kline_result['success'] or not kline_result['data']:
            return []

        formatted_data = []
        for kline in kline_result['data']:
            # OKX K线数据格式: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
            formatted_data.append({
                'timestamp': kline[0],
                'datetime': datetime.fromtimestamp(int(kline[0]) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'volume_currency': float(kline[6]),
                'volume_quote': float(kline[7]),
                'confirmed': kline[8] == '1'
            })

        return formatted_data

    def fetch_all_klines(self, csv_path=None, bar="1H", limit=9, delay=0.1):
        """
        批量获取所有交易对的K线数据

        Args:
            csv_path (str): CSV文件路径，如果为None则使用默认路径
            bar (str): K线周期
            limit (int): 每个交易对获取的K线数量
            delay (float): 请求间隔，避免频率限制

        Returns:
            dict: 所有K线数据结果
        """
        inst_ids = self.read_swap_csv(csv_path)[:2]
        if not inst_ids:
            return {}

        results = {}
        success_count = 0
        failed_count = 0

        print(f"🚀 开始获取 {len(inst_ids)} 个交易对的K线数据...")
        print(f"📊 参数: 周期={bar}, 数量={limit}根, 延迟={delay}秒")

        for i, inst_id in enumerate(inst_ids, 1):
            print(f"📈 [{i}/{len(inst_ids)}] 获取 {inst_id} 的K线数据...")

            result = self.get_kline_data(inst_id, bar, limit)
            results[inst_id] = result

            if result['success']:
                success_count += 1
                print(f"✅ 成功获取 {result['count']} 根K线")
            else:
                failed_count += 1
                print(f"❌ 获取失败: {result['error']}")

            # 避免请求频率过快
            if delay > 0:
                time.sleep(delay)

        print(f"\n📊 批量获取完成:")
        print(f"   ✅ 成功: {success_count} 个")
        print(f"   ❌ 失败: {failed_count} 个")
        print(f"   📈 成功率: {success_count / (success_count + failed_count) * 100:.1f}%")

        return results

    def save_klines_to_csv(self, kline_results, output_dir=None):
        """
        将K线数据保存为CSV文件

        Args:
            kline_results (dict): fetch_all_klines返回的结果
            output_dir (str): 输出目录，如果为None则使用默认路径
        """
        if output_dir is None:
            # 默认输出到 script/sqqq/kline_data/
            output_dir = os.path.join(self.script_dir, "kline_data")

        # 如果是相对路径，则相对于项目根目录
        if not os.path.isabs(output_dir):
            output_dir = os.path.join(self.project_root, output_dir)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📁 创建目录: {output_dir}")

        saved_count = 0

        for inst_id, result in kline_results.items():
            if result['success'] and result['data']:
                formatted_data = self.format_kline_data(result)
                if formatted_data:
                    df = pd.DataFrame(formatted_data)

                    # 文件名处理特殊字符
                    safe_filename = inst_id.replace('-', '_').replace('/', '_')
                    csv_path = os.path.join(output_dir, f"{safe_filename}_kline.csv")

                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    saved_count += 1
                    print(f"💾 已保存: {os.path.relpath(csv_path, self.project_root)}")

        print(f"\n📁 共保存 {saved_count} 个K线数据文件到 {os.path.relpath(output_dir, self.project_root)} 目录")




# 使用示例
def main():
    """
    主函数 - 演示如何使用OKXKlineFetcher
    """
    fetcher = OKXKlineFetcher()


    # 方法2: 从默认路径读取swap.csv并批量获取K线数据
    print("\n" + "=" * 50)
    print("🚀 批量获取K线数据")
    print("=" * 50)

    # 使用默认路径 script/sqqq/swap.csv
    results = fetcher.fetch_all_klines(bar="1H", limit=30, delay=0.1)

    if results:
        # 保存到默认路径 script/sqqq/kline_data/
        fetcher.save_klines_to_csv(results)

    # 方法3: 指定自定义路径
    print("\n" + "=" * 50)
    print("🔧 示例3: 使用自定义路径")
    print("=" * 50)

    # 可以使用相对路径或绝对路径
    # results = fetcher.fetch_all_klines("script/sqqq/swap.csv", bar="1H", limit=9)
    # fetcher.save_klines_to_csv(results, "script/sqqq/custom_kline_data")


# 如果要在proxy目录下的okproxy.py中使用，可以这样调用：
def run_kline_fetcher():
    """
    在okproxy.py中调用的简化函数
    """
    fetcher = OKXKlineFetcher()

    print("🚀 开始获取K线数据...")

    # 默认会读取 ../script/sqqq/swap.csv
    # 保存到 ../script/sqqq/kline_data/
    results = fetcher.fetch_all_klines(bar="1H", limit=30, delay=0.1)

    if results:
        fetcher.save_klines_to_csv(results)
        print("✅ K线数据获取完成!")
    else:
        print("❌ 没有获取到任何K线数据")

    return results


if __name__ == "__main__":
    main()