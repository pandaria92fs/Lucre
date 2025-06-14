import pandas as pd
import asyncio
import aiohttp
import time
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Any


class OKXKlineFetcher:
    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.kline_endpoint = "/api/v5/market/candles"

        # 获取当前脚本所在目录
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        # 项目根目录
        self.project_root = os.path.dirname(self.script_dir)

        # 并发控制参数
        self.max_concurrent = 5  # 最大并发数
        self.rate_limit = 5  # 每秒请求数
        self.semaphore = None  # 信号量，用于控制并发数

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

    async def get_kline_data_async(self, session: aiohttp.ClientSession, inst_id: str, bar: str = "1H", limit: int = 9):
        """
        异步获取指定交易对的K线数据

        Args:
            session (aiohttp.ClientSession): HTTP会话
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

        async with self.semaphore:  # 限制并发数
            try:
                async with session.get(url, params=params, timeout=10) as response:
                    response.raise_for_status()
                    data = await response.json()

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

            except Exception as e:
                return {
                    'instId': inst_id,
                    'success': False,
                    'error': f'请求失败: {str(e)}',
                    'data': []
                }

    def get_kline_data(self, inst_id, bar="1H", limit=9):
        """
        同步获取指定交易对的K线数据（保持向后兼容）

        Args:
            inst_id (str): 交易对ID，如 "BTC-USDT-SWAP"
            bar (str): K线周期，1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M/3M/6M/1Y
            limit (int): 获取的K线数量，默认9根（9小时）

        Returns:
            dict: K线数据
        """
        import requests

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

        except Exception as e:
            return {
                'instId': inst_id,
                'success': False,
                'error': f'请求失败: {str(e)}',
                'data': []
            }

    async def process_batch(self, session: aiohttp.ClientSession, batch: List[str], bar: str, limit: int):
        """
        处理一批交易对的K线数据获取

        Args:
            session: HTTP会话
            batch: 交易对ID列表
            bar: K线周期
            limit: K线数量

        Returns:
            dict: 批次处理结果
        """
        tasks = []
        for inst_id in batch:
            task = self.get_kline_data_async(session, inst_id, bar, limit)
            tasks.append(task)

        # 并发执行当前批次的所有任务
        results = await asyncio.gather(*tasks, return_exceptions=True)

        batch_results = {}
        for result in results:
            if isinstance(result, dict):
                batch_results[result['instId']] = result
            else:
                # 处理异常情况
                print(f"❌ 批次处理异常: {result}")

        return batch_results

    async def fetch_all_klines_async(self, csv_path=None, bar="1H", limit=9):
        """
        异步批量获取所有交易对的K线数据，控制并发数为每秒5个

        Args:
            csv_path (str): CSV文件路径，如果为None则使用默认路径
            bar (str): K线周期
            limit (int): 每个交易对获取的K线数量

        Returns:
            dict: 所有K线数据结果
        """
        inst_ids = self.read_swap_csv(csv_path)
        if not inst_ids:
            return {}

        # 初始化信号量，控制最大并发数
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        results = {}
        success_count = 0
        failed_count = 0

        print(f"🚀 开始异步获取 {len(inst_ids)} 个交易对的K线数据...")
        print(f"📊 参数: 周期={bar}, 数量={limit}根, 并发={self.max_concurrent}, 限速={self.rate_limit}/秒")

        # 创建批次，每批5个（每秒处理5个）
        batch_size = self.rate_limit
        batches = [inst_ids[i:i + batch_size] for i in range(0, len(inst_ids), batch_size)]

        start_time = time.time()

        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=self.max_concurrent)
        ) as session:

            for batch_idx, batch in enumerate(batches, 1):
                batch_start_time = time.time()

                print(f"📦 处理批次 [{batch_idx}/{len(batches)}] - {len(batch)} 个交易对")

                # 处理当前批次
                batch_results = await self.process_batch(session, batch, bar, limit)
                results.update(batch_results)

                # 统计结果
                for inst_id, result in batch_results.items():
                    if result['success']:
                        success_count += 1
                        print(f"✅ {inst_id}: 获取 {result['count']} 根K线")
                    else:
                        failed_count += 1
                        print(f"❌ {inst_id}: {result['error']}")

                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time

                # 如果这不是最后一批，且处理时间小于1秒，则等待
                if batch_idx < len(batches) and batch_duration < 1.0:
                    sleep_time = 1.0 - batch_duration
                    print(f"⏱️  批次完成用时 {batch_duration:.2f}s，等待 {sleep_time:.2f}s...")
                    await asyncio.sleep(sleep_time)

        total_time = time.time() - start_time
        print(f"\n📊 异步批量获取完成 (用时 {total_time:.2f}s):")
        print(f"   ✅ 成功: {success_count} 个")
        print(f"   ❌ 失败: {failed_count} 个")
        print(f"   📈 成功率: {success_count / (success_count + failed_count) * 100:.1f}%")
        print(f"   ⚡ 平均速度: {len(inst_ids) / total_time:.2f} 个/秒")

        return results

    def fetch_all_klines(self, csv_path=None, bar="1H", limit=9, delay=0.1, use_async=True):
        """
        批量获取所有交易对的K线数据

        Args:
            csv_path (str): CSV文件路径，如果为None则使用默认路径
            bar (str): K线周期
            limit (int): 每个交易对获取的K线数量
            delay (float): 同步模式下的请求间隔
            use_async (bool): 是否使用异步模式，默认True

        Returns:
            dict: 所有K线数据结果
        """
        if use_async:
            # 使用异步模式
            return asyncio.run(self.fetch_all_klines_async(csv_path, bar, limit))
        else:
            # 使用同步模式（原有逻辑）
            return self._fetch_all_klines_sync(csv_path, bar, limit, delay)

    def _fetch_all_klines_sync(self, csv_path=None, bar="1H", limit=9, delay=0.1):
        """
        同步模式批量获取（原有逻辑，保持向后兼容）
        """
        inst_ids = self.read_swap_csv(csv_path)
        if not inst_ids:
            return {}

        results = {}
        success_count = 0
        failed_count = 0

        print(f"🚀 开始同步获取 {len(inst_ids)} 个交易对的K线数据...")
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

        print(f"\n📊 同步批量获取完成:")
        print(f"   ✅ 成功: {success_count} 个")
        print(f"   ❌ 失败: {failed_count} 个")
        print(f"   📈 成功率: {success_count / (success_count + failed_count) * 100:.1f}%")

        return results

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

    def save_klines_to_csv(self, kline_results, output_dir=None):
        """
        将K线数据保存为CSV文件，按交易对分文件夹存储
        结构: kline_data_folder/交易对名称/时间戳.csv

        Args:
            kline_results (dict): fetch_all_klines返回的结果
            output_dir (str): 输出目录，如果为None则使用默认的kline_data_folder路径
        """
        if output_dir is None:
            # 默认输出到 service/kline_data_folder/
            output_dir = os.path.join(self.project_root, "service", "kline_data_folder")

        # 如果是相对路径，则相对于项目根目录
        if not os.path.isabs(output_dir):
            output_dir = os.path.join(self.project_root, output_dir)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📁 创建根目录: {output_dir}")

        saved_count = 0
        timestamp = datetime.now().strftime("%Y%m%d_%H")

        for inst_id, result in kline_results.items():
            if result['success'] and result['data']:
                formatted_data = self.format_kline_data(result)
                if formatted_data:
                    df = pd.DataFrame(formatted_data)

                    # 处理交易对名称，移除特殊字符作为文件夹名
                    trading_pair = inst_id.replace('-USDT-SWAP', '').replace('-', '').replace('/', '')

                    # 为每个交易对创建子文件夹
                    pair_folder = os.path.join(output_dir, trading_pair)
                    if not os.path.exists(pair_folder):
                        os.makedirs(pair_folder)
                        print(f"📁 创建交易对目录: {trading_pair}")

                    # 文件名：时间戳.csv
                    csv_filename = f"{timestamp}.csv"
                    csv_path = os.path.join(pair_folder, csv_filename)

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

    print("\n" + "=" * 50)
    print("🚀 并发批量获取K线数据 (每秒5个)")
    print("=" * 50)

    # 使用异步并发模式
    results = fetcher.fetch_all_klines(bar="1H", limit=30, use_async=True)

    if results:
        # 保存到默认路径
        fetcher.save_klines_to_csv(results)

    # 如果需要使用同步模式，可以这样调用：
    # results = fetcher.fetch_all_klines(bar="1H", limit=30, use_async=False, delay=0.2)


# 如果要在proxy目录下的okproxy.py中使用，可以这样调用：
def run_kline_fetcher():
    """
    在okproxy.py中调用的简化函数，使用并发模式
    """
    fetcher = OKXKlineFetcher()

    print("🚀 开始并发获取K线数据...")

    # 使用异步并发模式，每秒5个请求
    results = fetcher.fetch_all_klines(bar="1H", limit=30, use_async=True)

    if results:
        fetcher.save_klines_to_csv(results)
        print("✅ K线数据获取完成!")
    else:
        print("❌ 没有获取到任何K线数据")

    return results


if __name__ == "__main__":
    main()