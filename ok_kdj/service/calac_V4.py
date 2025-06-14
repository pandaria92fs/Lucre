import pandas as pd
import numpy as np
import warnings

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

    def process_csv_file(self, csv_path, target_values=None, save_all_methods=False):
        """
        处理CSV文件

        Args:
            csv_path: 输入CSV文件路径
            target_values: 目标值元组 (k, d, j)，可选
            save_all_methods: 是否保存所有方法的结果
        """
        try:
            # 读取数据
            df = pd.read_csv(csv_path)

            required_columns = ['timestamp', 'open', 'high', 'low', 'close']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                print(f"❌ 缺少必要列: {missing_columns}")
                return None

            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

            print(f"📂 处理文件: {csv_path}")
            print(f"📊 数据点数: {len(df)}")
            print(f"📅 时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}")

            # 测试所有方法
            if target_values:
                best_result, best_method = self.compare_with_target(df, *target_values)
            else:
                results = self.test_all_methods(df)
                # 默认使用元宝方法
                best_result = results["方法1_元宝风格"]['data']
                best_method = "方法1_元宝风格"

            # 显示最新几个值
            if best_result is not None:
                print(f"\n📈 最新KDJ值 (使用{best_method}):")
                latest_data = best_result[['datetime', 'close', 'RSV', 'K', 'D', 'J']].tail(3)

                for _, row in latest_data.iterrows():
                    print(f"{row['datetime']:%Y-%m-%d %H:%M} | "
                          f"Close: {row['close']:8.4f} | "
                          f"RSV: {row['RSV']:6.2f} | "
                          f"K: {row['K']:7.4f} | "
                          f"D: {row['D']:7.4f} | "
                          f"J: {row['J']:8.4f}")

                # 保存最佳结果
                output_path = csv_path.replace('.csv', '_direct_kdj.csv')
                save_columns = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'RSV', 'K', 'D', 'J']
                available_columns = [col for col in save_columns if col in best_result.columns]

                best_result[available_columns].to_csv(output_path, index=False, encoding='utf-8')
                print(f"💾 最佳结果已保存到: {output_path}")

                return best_result

        except Exception as e:
            print(f"❌ 处理失败: {str(e)}")
            return None


def main():
    """主函数 - 直接计算演示"""

    print("🔍 直接计算KDJ - 多方法对比")
    print("=" * 60)
    print("📊 将测试5种不同的KDJ计算方法:")
    print("   1️⃣ 元宝风格 (固定初始值50)")
    print("   2️⃣ 首个RSV初始化")
    print("   3️⃣ 滚动平均初始化")
    print("   4️⃣ 简单移动平均")
    print("   5️⃣ 指数移动平均")
    print("=" * 60)

    # 创建计算器
    calculator = DirectKDJCalculator()
    # 测试文件
    sample_file = "/ok_kdj/service/kdj_folder/BTCUSDT/BTCUSDT_24rows_20250614_10_beijing_kdj.csv"

    import os
    if os.path.exists(sample_file):
        print(f"\n📂 测试文件: {sample_file}")

        # 不使用任何目标值，纯粹计算对比
        result = calculator.process_csv_file(sample_file, target_values=None)

        if result is not None:
            print(f"\n✅ 直接计算完成!")
            print(f"📊 所有方法的结果已展示，可以看出不同方法的差异")
            print(f"💡 如果有欧意的实际值，可以传入target_values参数进行对比")

        # 如果你想与欧意值对比，取消下面注释
        # okx_values = (75.3456, 71.0386, 83.9595)
        # result = calculator.process_csv_file(sample_file, target_values=okx_values)

    else:
        print(f"❌ 测试文件不存在: {sample_file}")

        # 使用示例数据演示
        print(f"\n📊 使用示例数据演示:")
        sample_data = pd.DataFrame({
            'timestamp': range(1000000000000, 1000000000000 + 10 * 3600 * 1000, 3600 * 1000),
            'high': [127.01, 127.62, 128.43, 128.47, 127.73, 126.38, 125.64, 126.59, 126.82, 126.58],
            'low': [126.25, 126.61, 127.16, 127.71, 126.84, 125.94, 124.84, 125.58, 126.13, 125.62],
            'close': [126.85, 127.45, 128.08, 127.75, 126.91, 126.12, 125.26, 126.11, 126.59, 126.07],
            'open': [126.50, 126.85, 127.45, 128.08, 127.75, 126.91, 126.12, 125.26, 126.11, 126.59]
        })

        results = calculator.test_all_methods(sample_data)
        print(f"\n✅ 示例数据计算完成!")


if __name__ == "__main__":
    main()