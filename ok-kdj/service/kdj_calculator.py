import pandas as pd
import numpy as np
import warnings
import os
from datetime import datetime

warnings.filterwarnings('ignore')


class DirectKDJCalculator:
    def __init__(self, rsv_period=9):
        """
        直接计算KDJ - 不使用任何预设值或假设

        Args:
            rsv_period (int): RSV计算周期，默认9
        """
        self.rsv_period = rsv_period
        print(f"📊 直接计算KDJ - RSV周期: {rsv_period}")

    def calculate_kdj_method1_yuanbao(self, df):
        """
        方法1: 元宝风格 - 简洁直接
        """
        data = df.copy().sort_values('timestamp').reset_index(drop=True)

        # 计算滚动最高最低价
        data['min_low'] = data['low'].rolling(window=self.rsv_period, min_periods=1).min()
        data['max_high'] = data['high'].rolling(window=self.rsv_period, min_periods=1).max()

        # 计算RSV
        price_range = data['max_high'] - data['min_low']
        data['RSV'] = np.where(
            price_range == 0,
            50.0,
            (data['close'] - data['min_low']) / price_range * 100
        )
        data['RSV'].fillna(50, inplace=True)

        # 初始化K、D值为50
        data['K'] = 50.0
        data['D'] = 50.0

        # 迭代计算
        for i in range(1, len(data)):
            data.loc[i, 'K'] = (2 / 3) * data.loc[i - 1, 'K'] + (1 / 3) * data.loc[i, 'RSV']
            data.loc[i, 'D'] = (2 / 3) * data.loc[i - 1, 'D'] + (1 / 3) * data.loc[i, 'K']

        # 计算J值
        data['J'] = 3 * data['K'] - 2 * data['D']

        return data

    def calculate_kdj_method2_first_rsv(self, df):
        """
        方法2: 使用第一个RSV作为初始值
        """
        data = df.copy().sort_values('timestamp').reset_index(drop=True)

        # 计算RSV
        data['min_low'] = data['low'].rolling(window=self.rsv_period, min_periods=self.rsv_period).min()
        data['max_high'] = data['high'].rolling(window=self.rsv_period, min_periods=self.rsv_period).max()

        price_range = data['max_high'] - data['min_low']
        rsv = np.where(
            (price_range == 0) | pd.isna(price_range),
            50.0,
            (data['close'] - data['min_low']) / price_range * 100
        )

        data['RSV'] = rsv

        # 找到第一个非NaN的RSV作为初始值
        first_valid_rsv = None
        first_valid_idx = 0

        for i in range(len(data)):
            if not pd.isna(data['RSV'].iloc[i]):
                first_valid_rsv = data['RSV'].iloc[i]
                first_valid_idx = i
                break

        if first_valid_rsv is None:
            first_valid_rsv = 50.0

        # 初始化K、D值
        k_values = np.full(len(data), np.nan)
        d_values = np.full(len(data), np.nan)

        # 设置初始值
        k_values[first_valid_idx] = first_valid_rsv
        d_values[first_valid_idx] = first_valid_rsv

        # 迭代计算
        for i in range(first_valid_idx + 1, len(data)):
            if not pd.isna(data['RSV'].iloc[i]):
                k_values[i] = (2 / 3) * k_values[i - 1] + (1 / 3) * data['RSV'].iloc[i]
                d_values[i] = (2 / 3) * d_values[i - 1] + (1 / 3) * k_values[i]
            else:
                k_values[i] = k_values[i - 1]
                d_values[i] = d_values[i - 1]

        data['K'] = k_values
        data['D'] = d_values
        data['J'] = 3 * data['K'] - 2 * data['D']

        return data

    def calculate_kdj_method3_rolling_init(self, df):
        """
        方法3: 使用滚动平均作为初始值
        """
        data = df.copy().sort_values('timestamp').reset_index(drop=True)

        # 计算RSV
        data['min_low'] = data['low'].rolling(window=self.rsv_period, min_periods=1).min()
        data['max_high'] = data['high'].rolling(window=self.rsv_period, min_periods=1).max()

        price_range = data['max_high'] - data['min_low']
        data['RSV'] = np.where(
            price_range == 0,
            50.0,
            (data['close'] - data['min_low']) / price_range * 100
        )

        # 使用前几个RSV的平均值作为初始K、D值
        init_period = min(3, len(data))
        initial_value = data['RSV'].head(init_period).mean()
        if pd.isna(initial_value):
            initial_value = 50.0

        data['K'] = initial_value
        data['D'] = initial_value

        # 迭代计算
        for i in range(1, len(data)):
            data.loc[i, 'K'] = (2 / 3) * data.loc[i - 1, 'K'] + (1 / 3) * data.loc[i, 'RSV']
            data.loc[i, 'D'] = (2 / 3) * data.loc[i - 1, 'D'] + (1 / 3) * data.loc[i, 'K']

        data['J'] = 3 * data['K'] - 2 * data['D']

        return data

    def calculate_kdj_method4_sma(self, df):
        """
        方法4: 使用简单移动平均
        """
        data = df.copy().sort_values('timestamp').reset_index(drop=True)

        # 计算RSV
        data['min_low'] = data['low'].rolling(window=self.rsv_period, min_periods=1).min()
        data['max_high'] = data['high'].rolling(window=self.rsv_period, min_periods=1).max()

        price_range = data['max_high'] - data['min_low']
        data['RSV'] = np.where(
            price_range == 0,
            50.0,
            (data['close'] - data['min_low']) / price_range * 100
        )
        data['RSV'].fillna(50, inplace=True)

        # 使用SMA计算K和D
        data['K'] = data['RSV'].rolling(window=3, min_periods=1).mean()
        data['D'] = data['K'].rolling(window=3, min_periods=1).mean()
        data['J'] = 3 * data['K'] - 2 * data['D']

        return data

    def calculate_kdj_method5_ema(self, df):
        """
        方法5: 使用指数移动平均
        """
        data = df.copy().sort_values('timestamp').reset_index(drop=True)

        # 计算RSV
        data['min_low'] = data['low'].rolling(window=self.rsv_period, min_periods=1).min()
        data['max_high'] = data['high'].rolling(window=self.rsv_period, min_periods=1).max()

        price_range = data['max_high'] - data['min_low']
        data['RSV'] = np.where(
            price_range == 0,
            50.0,
            (data['close'] - data['min_low']) / price_range * 100
        )
        data['RSV'].fillna(50, inplace=True)

        # 使用EMA计算K和D
        data['K'] = data['RSV'].ewm(span=3, adjust=False).mean()
        data['D'] = data['K'].ewm(span=3, adjust=False).mean()
        data['J'] = 3 * data['K'] - 2 * data['D']

        return data

    def test_all_methods(self, df):
        """
        测试所有方法并显示结果
        """
        print(f"\n🧪 测试所有KDJ计算方法:")
        print("=" * 80)

        methods = [
            ("方法1_元宝风格", self.calculate_kdj_method1_yuanbao),
            ("方法2_首个RSV初始化", self.calculate_kdj_method2_first_rsv),
            ("方法3_滚动平均初始化", self.calculate_kdj_method3_rolling_init),
            ("方法4_简单移动平均", self.calculate_kdj_method4_sma),
            ("方法5_指数移动平均", self.calculate_kdj_method5_ema)
        ]

        results = {}

        for method_name, method_func in methods:
            try:
                result = method_func(df)
                if len(result) > 0:
                    latest = result.iloc[-1]
                    results[method_name] = {
                        'data': result,
                        'K': latest['K'],
                        'D': latest['D'],
                        'J': latest['J']
                    }

                    print(f"{method_name:<20} | K:{latest['K']:8.4f} | D:{latest['D']:8.4f} | J:{latest['J']:9.4f}")
                else:
                    print(f"{method_name:<20} | 无数据")

            except Exception as e:
                print(f"{method_name:<20} | 计算失败: {str(e)}")

        return results

    def compare_with_target(self, df, target_k=None, target_d=None, target_j=None):
        """
        如果有目标值，进行对比
        """
        results = self.test_all_methods(df)

        if target_k is not None and target_d is not None and target_j is not None:
            print(f"\n🎯 与目标值对比 (K={target_k:.4f}, D={target_d:.4f}, J={target_j:.4f}):")
            print("=" * 90)
            print(f"{'方法':<20} {'K值':<10} {'D值':<10} {'J值':<10} {'K误差':<8} {'D误差':<8} {'J误差':<8} {'总误差'}")
            print("-" * 90)

            best_method = None
            best_error = float('inf')

            for method_name, result in results.items():
                k_error = abs(result['K'] - target_k)
                d_error = abs(result['D'] - target_d)
                j_error = abs(result['J'] - target_j)
                total_error = k_error + d_error + j_error

                status = "🏆" if total_error < best_error else ""

                if total_error < best_error:
                    best_error = total_error
                    best_method = method_name

                print(f"{method_name:<20} {result['K']:<10.4f} {result['D']:<10.4f} {result['J']:<10.4f} "
                      f"{k_error:<8.4f} {d_error:<8.4f} {j_error:<8.4f} {total_error:<8.4f} {status}")

            print("-" * 90)
            print(f"🏆 最佳方法: {best_method} (总误差: {best_error:.4f})")

            return results[best_method]['data'], best_method

        return results, None

    def process_single_file(self, csv_path, output_path, method='yuanbao'):
        """
        处理单个CSV文件，使用指定方法计算KDJ

        Args:
            csv_path: 输入CSV文件路径
            output_path: 输出CSV文件路径
            method: 使用的计算方法，默认元宝风格
        """
        try:
            # 读取数据
            df = pd.read_csv(csv_path)

            required_columns = ['timestamp', 'open', 'high', 'low', 'close']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                print(f"❌ 缺少必要列: {missing_columns}")
                return False

            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

            print(f"\n📂 处理文件: {os.path.basename(csv_path)}")
            print(f"📊 数据点数: {len(df)}")

            # 使用元宝方法计算KDJ
            result = self.calculate_kdj_method1_yuanbao(df)

            # 显示最新值
            if len(result) > 0:
                latest = result.iloc[-1]
                print(f"📈 最新KDJ值: K={latest['K']:.4f}, D={latest['D']:.4f}, J={latest['J']:.4f}")

                # 保存结果
                save_columns = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume',
                                'vol_ccy_quote', 'RSV', 'K', 'D', 'J']
                # 只保存存在的列
                available_columns = [col for col in save_columns if col in result.columns]

                result[available_columns].to_csv(output_path, index=False, encoding='utf-8')
                print(f"💾 结果已保存")

                return True

        except Exception as e:
            print(f"❌ 处理失败: {str(e)}")
            return False


def batch_process_kdj():
    """
    批量处理kline_folder下的所有K线数据，计算KDJ并保存到kdj_folder
    """
    # 定义文件夹路径
    kline_folder = os.path.join("..", "service", "kline_folder")
    kdj_folder = os.path.join("..", "service", "kdj_folder")

    # 创建kdj_folder
    if not os.path.exists(kdj_folder):
        os.makedirs(kdj_folder)
        print(f"📁 已创建目录: {kdj_folder}")

    # 创建KDJ计算器
    calculator = DirectKDJCalculator(rsv_period=9)

    print("🚀 开始批量处理K线数据，计算KDJ指标")
    print("=" * 80)

    # 获取所有交易对文件夹
    if not os.path.exists(kline_folder):
        print(f"❌ K线数据目录不存在: {kline_folder}")
        return

    # 统计处理结果
    total_files = 0
    success_files = 0
    failed_files = 0

    # 遍历kline_folder中的所有子文件夹
    for symbol_folder in os.listdir(kline_folder):
        symbol_folder_path = os.path.join(kline_folder, symbol_folder)

        # 确保是文件夹
        if not os.path.isdir(symbol_folder_path):
            continue

        print(f"\n📂 处理交易对: {symbol_folder}")
        print("-" * 40)

        # 创建对应的kdj文件夹
        kdj_symbol_folder = os.path.join(kdj_folder, symbol_folder)
        if not os.path.exists(kdj_symbol_folder):
            os.makedirs(kdj_symbol_folder)

        # 获取该文件夹下的所有CSV文件
        csv_files = [f for f in os.listdir(symbol_folder_path) if f.endswith('.csv')]

        for csv_file in csv_files:
            total_files += 1

            # 输入输出文件路径
            input_path = os.path.join(symbol_folder_path, csv_file)

            # 修改输出文件名，添加_kdj后缀
            output_filename = csv_file.replace('.csv', '_kdj.csv')
            output_path = os.path.join(kdj_symbol_folder, output_filename)

            # 处理文件
            if calculator.process_single_file(input_path, output_path):
                success_files += 1
            else:
                failed_files += 1

    # 显示处理结果总结
    print("\n" + "=" * 80)
    print("📊 批量处理完成!")
    print(f"📈 总文件数: {total_files}")
    print(f"✅ 成功处理: {success_files}")
    print(f"❌ 处理失败: {failed_files}")
    print(f"📁 结果保存在: {kdj_folder}")
    print("=" * 80)


def main():
    """主函数 - 批量处理K线数据"""
    print("🔍 KDJ批量处理程序")
    print("=" * 60)
    print("📊 使用元宝风格算法计算KDJ指标")
    print("📁 输入目录: ../service/kline_folder/")
    print("📁 输出目录: ../service/kdj_folder/")
    print("=" * 60)

    # 执行批量处理
    batch_process_kdj()

    print("\n✨ 程序执行完毕!")


if __name__ == "__main__":
    main()