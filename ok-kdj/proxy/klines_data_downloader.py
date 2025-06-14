import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import json
import os
from typing import Optional, List


class OKXKlineDownloader:
    """
    OKX K线数据下载器
    支持下载各种时间周期的K线数据
    """

    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.api_url = "/api/v5/market/history-candles"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_kline_data(self,
                       symbol: str,
                       bar: str = "1D",
                       after: Optional[str] = None,
                       before: Optional[str] = None,
                       limit: int = 100) -> dict:
        """
        获取K线数据

        Args:
            symbol: 交易对，如 "BTC-USDT"
            bar: K线周期 (1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M, 3M, 6M, 1Y)
            after: 请求此时间戳之前的数据
            before: 请求此时间戳之后的数据
            limit: 返回结果的数量，最大值为100

        Returns:
            dict: API响应数据
        """
        url = self.base_url + self.api_url

        params = {
            'instId': symbol,
            'bar': bar,
            'limit': str(limit)
        }

        if after:
            params['after'] = after
        if before:
            params['before'] = before

        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return None

    def timestamp_to_datetime(self, timestamp: str) -> datetime:
        """将时间戳转换为datetime对象"""
        return datetime.fromtimestamp(int(timestamp) / 1000)

    def datetime_to_timestamp(self, dt: datetime) -> str:
        """将datetime对象转换为时间戳"""
        return str(int(dt.timestamp() * 1000))

    def download_historical_data(self,
                                 symbol: str,
                                 bar: str = "1H",
                                 start_date: str = None,
                                 end_date: str = None,
                                 output_file: str = None) -> pd.DataFrame:
        """
        下载历史K线数据

        Args:
            symbol: 交易对，如 "BTC-USDT"
            bar: K线周期
            start_date: 开始日期 (格式: "2023-01-01")
            end_date: 结束日期 (格式: "2023-12-31")
            output_file: 输出文件名

        Returns:
            pd.DataFrame: K线数据
        """
        print(f"开始下载 {symbol} {bar} K线数据...")

        all_data = []
        after = None

        # 设置结束时间
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            before = self.datetime_to_timestamp(end_dt)
        else:
            before = None

        # 设置开始时间
        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            target_timestamp = self.datetime_to_timestamp(start_dt)
        else:
            target_timestamp = None

        page_count = 0

        while True:
            print(f"正在获取第 {page_count + 1} 页数据...")

            # 获取数据
            data = self.get_kline_data(
                symbol=symbol,
                bar=bar,
                after=after,
                before=before,
                limit=100
            )

            if not data or data.get('code') != '0':
                print(f"获取数据失败: {data}")
                break

            klines = data.get('data', [])

            if not klines:
                print("没有更多数据")
                break

            # 检查是否到达目标开始时间
            if target_timestamp:
                filtered_klines = []
                for kline in klines:
                    if kline[0] >= target_timestamp:
                        filtered_klines.append(kline)
                    else:
                        break

                all_data.extend(filtered_klines)

                # 如果找到了开始时间之前的数据，停止获取
                if len(filtered_klines) < len(klines):
                    break
            else:
                all_data.extend(klines)

            # 设置下一页的after参数
            after = klines[-1][0]
            page_count += 1

            # 避免请求过于频繁
            time.sleep(0.1)

            # 安全检查，避免无限循环
            if page_count > 1000:
                print("已达到最大页数限制")
                break

        if not all_data:
            print("没有获取到数据")
            return pd.DataFrame()

        # 转换为DataFrame
        df = self.convert_to_dataframe(all_data)

        # 按时间排序
        df = df.sort_values('timestamp').reset_index(drop=True)

        print(f"成功获取 {len(df)} 条K线数据")
        print(f"时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}")

        # 保存到文件
        if output_file:
            self.save_to_file(df, output_file)
        else:
            self.save_to_file(df, symbol=symbol)

        return df

    def convert_to_dataframe(self, klines: List) -> pd.DataFrame:
        """
        将K线数据转换为DataFrame

        数据格式: [timestamp, open, high, low, close, volume, volCcy, volCcyQuote, confirm]
        """
        # 原始数据包含9列，但我们只保留需要的列
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vol_ccy', 'vol_ccy_quote', 'confirm']

        df = pd.DataFrame(klines, columns=columns)

        # 转换数据类型
        df['timestamp'] = df['timestamp'].astype(int)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['vol_ccy_quote'] = df['vol_ccy_quote'].astype(float)

        # 添加日期时间列
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

        # 删除重复的vol_ccy列，只保留volume
        df = df.drop('vol_ccy', axis=1)

        return df

    def save_to_file(self, df: pd.DataFrame, filename: str = None, symbol: str = None):
        """保存数据到CSV文件"""
        if filename is None and symbol:
            # 自动生成文件名：交易对+数据行数+当前北京时间
            from datetime import timezone
            beijing_tz = timezone(timedelta(hours=8))
            current_beijing = datetime.now(beijing_tz)
            date_hour = current_beijing.strftime("%Y%m%d_%H")
            data_rows = len(df)
            filename = f"{symbol.replace('-', '')}_{data_rows}rows_{date_hour}_beijing.csv"

        # 确保文件扩展名为.csv
        if not filename.endswith('.csv'):
            filename = filename.rsplit('.', 1)[0] + '.csv'

        # 创建相对于proxy目录的kline_folder路径
        # 如果当前在proxy目录下运行，则在service/kline_folder下保存
        kline_folder = os.path.join("..", "service", "kline_folder")

        if not os.path.exists(kline_folder):
            os.makedirs(kline_folder)
            print(f"已创建目录: {kline_folder}")

        # 为每个交易对创建专门的文件夹
        if symbol:
            symbol_folder_name = symbol.replace('-', '')  # BTC-USDT -> BTCUSDT
            symbol_folder_path = os.path.join(kline_folder, symbol_folder_name)

            if not os.path.exists(symbol_folder_path):
                os.makedirs(symbol_folder_path)
                print(f"已创建交易对目录: {symbol_folder_path}")

            # 完整的文件路径
            filepath = os.path.join(symbol_folder_path, filename)
        else:
            # 如果没有指定symbol，直接保存到kline_folder根目录
            filepath = os.path.join(kline_folder, filename)

        df.to_csv(filepath, index=False)
        print(f"数据已保存到: {filepath}")

    def download_recent_data(self,
                             symbol: str,
                             bar: str = "1H",
                             hours: int = 12,
                             output_file: str = None) -> pd.DataFrame:
        """
        下载最近几小时的K线数据

        Args:
            symbol: 交易对，如 "BTC-USDT"
            bar: K线周期，默认1小时
            hours: 过去多少小时的数据，默认12小时
            output_file: 输出文件名

        Returns:
            pd.DataFrame: K线数据
        """
        print(f"开始下载 {symbol} 过去{hours}小时的 {bar} K线数据...")

        # 获取当前北京时间
        from datetime import timezone
        beijing_tz = timezone(timedelta(hours=8))
        current_beijing = datetime.now(beijing_tz)
        print(f"当前北京时间: {current_beijing.strftime('%Y-%m-%d %H:%M:%S')}")

        # 计算开始时间（过去12小时）
        start_beijing = current_beijing - timedelta(hours=hours)
        print(f"获取数据开始时间（北京时间）: {start_beijing.strftime('%Y-%m-%d %H:%M:%S')}")

        # 转换为时间戳
        end_timestamp = self.datetime_to_timestamp(current_beijing.replace(tzinfo=None))
        start_timestamp = self.datetime_to_timestamp(start_beijing.replace(tzinfo=None))

        # 获取数据
        all_data = []
        after = None
        page_count = 0

        while True:
            print(f"正在获取第 {page_count + 1} 页数据...")

            data = self.get_kline_data(
                symbol=symbol,
                bar=bar,
                after=after,
                before=end_timestamp,
                limit=100
            )

            if not data or data.get('code') != '0':
                print(f"获取数据失败: {data}")
                break

            klines = data.get('data', [])

            if not klines:
                print("没有更多数据")
                break

            # 过滤时间范围内的数据
            filtered_klines = []
            for kline in klines:
                if kline[0] >= start_timestamp and kline[0] <= end_timestamp:
                    filtered_klines.append(kline)
                elif kline[0] < start_timestamp:
                    break

            all_data.extend(filtered_klines)

            # 检查是否已获取到开始时间之前的数据
            if klines[-1][0] < start_timestamp:
                break

            # 设置下一页的after参数
            after = klines[-1][0]
            page_count += 1

            # 避免请求过于频繁
            time.sleep(0.1)

            # 安全检查
            if page_count > 10:
                break

        if not all_data:
            print("没有获取到数据")
            return pd.DataFrame()

        # 转换为DataFrame
        df = self.convert_to_dataframe(all_data)

        # 按时间排序
        df = df.sort_values('timestamp').reset_index(drop=True)

        # 显示北京时间范围
        df['beijing_time'] = pd.to_datetime(df['timestamp'], unit='ms') + timedelta(hours=8)

        print(f"成功获取 {len(df)} 条K线数据")
        print(f"时间范围（UTC）: {df['datetime'].min()} 到 {df['datetime'].max()}")
        print(f"时间范围（北京）: {df['beijing_time'].min()} 到 {df['beijing_time'].max()}")

        # 删除临时列
        df = df.drop('beijing_time', axis=1)

        # 保存到文件
        if output_file:
            self.save_to_file(df, output_file)
        else:
            self.save_to_file(df, symbol=symbol)

        return df

    def get_available_symbols(self) -> List[str]:
        """获取可用的交易对列表"""
        url = self.base_url + "/api/v5/public/instruments"
        params = {'instType': 'SPOT'}

        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == '0':
                instruments = data.get('data', [])
                symbols = [inst['instId'] for inst in instruments]
                return symbols
            else:
                print(f"获取交易对失败: {data}")
                return []

        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return []


def main():
    """主函数 - 使用示例"""
    downloader = OKXKlineDownloader()

    # 配置参数 - 获取过去12小时的数据
    symbol = "BTC-USDT"  # 交易对
    bar = "1H"  # K线周期 - 1小时
    hours = 12  # 过去12小时

    # 显示当前北京时间
    from datetime import timezone
    beijing_tz = timezone(timedelta(hours=8))
    current_beijing = datetime.now(beijing_tz)

    print("=" * 60)
    print(f"🕐 当前北京时间: {current_beijing.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 准备获取 {symbol} 过去{hours}小时的{bar}级别K线数据")
    print("=" * 60)

    # 使用新的方法下载最近数据
    df = downloader.download_recent_data(
        symbol=symbol,
        bar=bar,
        hours=hours
        # output_file 参数留空，自动生成文件名
    )

    # 显示基本信息
    if not df.empty:
        print("\n数据预览:")
        print(df.head())
        print(f"\n数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")

        # 显示具体的时间范围（北京时间）
        df_beijing = df.copy()
        df_beijing['beijing_time'] = pd.to_datetime(df_beijing['timestamp'], unit='ms') + timedelta(hours=8)

        print(f"\n实际数据时间范围:")
        print(f"最早（UTC）: {df['datetime'].min()}")
        print(f"最新（UTC）: {df['datetime'].max()}")
        print(f"最早（北京）: {df_beijing['beijing_time'].min()}")
        print(f"最新（北京）: {df_beijing['beijing_time'].max()}")

        # 显示文件命名格式说明
        example_filename = f"{symbol.replace('-', '')}_{len(df)}rows_{current_beijing.strftime('%Y%m%d_%H')}_beijing.csv"
        symbol_folder = symbol.replace('-', '')
        print(f"\n文件保存路径: ../service/kline_folder/{symbol_folder}/{example_filename}")
        print(f"✅ 过去{hours}小时的K线数据已保存")
    else:
        print("未获取到数据")


if __name__ == "__main__":
    main()