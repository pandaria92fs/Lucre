import pandas as pd
import numpy as np
from datetime import datetime


class SQQQBacktest:
    def __init__(self, csv_path):
        self.data = self._load_csv(csv_path)

    def _load_csv(self, csv_path):
        try:
            df = pd.read_csv(csv_path, index_col=0)
            df.index = pd.to_datetime(df.index)
            return df.sort_index()
        except Exception as e:
            raise Exception(f"读取CSV文件错误: {str(e)}")

    def run_backtest(self, start_date, end_date, initial_capital):
        try:
            # 获取时间段内的数据
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            mask = (self.data.index >= start) & (self.data.index <= end)
            period_data = self.data[mask].copy()

            if len(period_data) == 0:
                return {"error": "所选时间段内没有数据"}

            # 基础数据
            initial_price = period_data['Close'].iloc[0]
            final_price = period_data['Close'].iloc[-1]

            # 做空收益率计算
            total_return_pct = ((initial_price - final_price) / initial_price) * 100

            # 计算时间
            years = (end - start).days / 365.25

            # 计算做空成本（年化5%的借贷成本）
            borrowing_cost_rate = 0.05  # 5%年化借贷成本
            borrowing_cost = borrowing_cost_rate * years * 100

            # 计算净收益
            net_return = total_return_pct - borrowing_cost

            # 计算年化收益率
            if net_return >= 0:
                annualized_return = (pow(1 + net_return / 100, 1 / years) - 1) * 100
            else:
                annualized_return = -(pow(1 - net_return / 100, 1 / years) - 1) * 100

            # 计算每日收益率序列（用于计算其他指标）
            # 修复：明确指定dtype为float64
            daily_returns = pd.Series(np.zeros(len(period_data)), index=period_data.index, dtype='float64')

            for i in range(1, len(period_data)):
                prev_price = period_data['Close'].iloc[i - 1]
                curr_price = period_data['Close'].iloc[i]
                daily_return = ((prev_price - curr_price) / prev_price) * 100
                daily_returns.iloc[i] = daily_return

            results = {
                'start_date': start_date,
                'end_date': end_date,
                'initial_capital': initial_capital,
                'initial_price': initial_price,
                'final_price': final_price,
                'trading_days': len(period_data),
                'price_change_pct': ((final_price - initial_price) / initial_price) * 100,
                'total_return_pct': total_return_pct,
                'borrowing_cost_pct': borrowing_cost,
                'net_return_pct': net_return,
                'annualized_return': annualized_return,
                'final_capital': initial_capital * (1 + net_return / 100),
                'avg_daily_return': daily_returns.mean(),
                'return_std': daily_returns.std(),
                'max_daily_loss': daily_returns.min(),
                'max_daily_gain': daily_returns.max(),
                'sharpe_ratio': self._calculate_sharpe_ratio(daily_returns),
                'max_drawdown': self._calculate_max_drawdown(period_data['Close'])
            }

            return results

        except Exception as e:
            return {"error": f"运行回测时出错: {str(e)}"}

    def _calculate_sharpe_ratio(self, returns, risk_free_rate=0.02):
        """计算夏普比率"""
        excess_returns = returns / 100 - risk_free_rate / 252
        if excess_returns.std() == 0:
            return 0
        return np.sqrt(252) * excess_returns.mean() / excess_returns.std()

    def _calculate_max_drawdown(self, prices):
        """计算最大回撤"""
        rolling_max = prices.expanding().max()
        drawdowns = (prices - rolling_max) / rolling_max * 100
        return drawdowns.min()


def format_results(results):
    if "error" in results:
        return f"错误: {results['error']}"

    output = f"""
SQQQ做空回测结果:
====================
基本信息:
- 回测区间: {results['start_date']} 至 {results['end_date']}
- 交易天数: {results['trading_days']}天
- 初始资金: ${results['initial_capital']:,.2f}
- 最终资金: ${results['final_capital']:,.2f}

价格变动:
- 开始价格: ${results['initial_price']:.2f}
- 结束价格: ${results['final_price']:.2f}
- SQQQ变动: {results['price_change_pct']:.2f}%

收益统计:
- 做空总收益: {results['total_return_pct']:.2f}%
- 借贷成本: {results['borrowing_cost_pct']:.2f}%
- 净收益率: {results['net_return_pct']:.2f}%
- 年化收益率: {results['annualized_return']:.2f}%
- 平均日收益: {results['avg_daily_return']:.2f}%

风险指标:
- 收益标准差: {results['return_std']:.2f}%
- 最大日亏损: {results['max_daily_loss']:.2f}%
- 最大日盈利: {results['max_daily_gain']:.2f}%
- 最大回撤: {results['max_drawdown']:.2f}%
- 夏普比率: {results['sharpe_ratio']:.2f}
"""
    return output


if __name__ == "__main__":
    try:
        print("开始加载数据...")
        backtest = SQQQBacktest('sqqq_data.csv')
        print("数据加载完成")

        print("开始运行回测...")
        results = backtest.run_backtest(
            start_date='2024-01-01',
            end_date='2025-02-14',
            initial_capital=100000
        )

        print(format_results(results))

    except Exception as e:
        print(f"程序执行错误: {str(e)}")