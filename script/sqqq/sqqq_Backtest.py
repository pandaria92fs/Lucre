import pandas as pd
import numpy as np
from datetime import datetime


class SQQQBacktest:
    def __init__(self, csv_path):
        self.data = self._load_csv(csv_path)

    def _load_csv(self, csv_path):
        try:
            df = pd.read_csv(csv_path, index_col=0)
            return self._prepare_data(df)
        except Exception as e:
            raise Exception(f"读取CSV文件错误: {str(e)}")

    def _prepare_data(self, df):
        try:
            required_columns = ['Open', 'High', 'Low', 'Close']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"缺少必要的列: {col}")

            df.index = pd.to_datetime(df.index)
            df = df.sort_index()

            numeric_columns = ['Open', 'High', 'Low', 'Close']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            return df

        except Exception as e:
            raise Exception(f"处理数据时出错: {str(e)}")

    def _calculate_annualized_return(self, total_return, years):
        """
        计算年化收益率
        处理正收益和负收益的情况
        """
        if total_return >= 0:
            return (pow(1 + total_return / 100, 1 / years) - 1) * 100
        else:
            # 对于负收益，使用调整后的计算方法
            return -((pow(1 - total_return / 100, 1 / years) - 1) * 100)

    def run_backtest(self, start_date, end_date, initial_capital):
        try:
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)

            mask = (self.data.index >= start) & (self.data.index <= end)
            period_data = self.data[mask].copy()

            if len(period_data) == 0:
                return {"error": "所选时间段内没有数据"}

            period_data['Daily_Return'] = -period_data['Close'].pct_change() * 100
            period_data['Cumulative_Return'] = (1 + period_data['Daily_Return'] / 100).cumprod() - 1
            period_data['Portfolio_Value'] = initial_capital * (1 + period_data['Cumulative_Return'])

            total_return = ((period_data['Portfolio_Value'].iloc[-1] / initial_capital) - 1) * 100
            years = (end - start).days / 365.25

            results = {
                'start_date': start_date,
                'end_date': end_date,
                'initial_capital': initial_capital,
                'trading_days': len(period_data),
                'final_capital': period_data['Portfolio_Value'].iloc[-1],
                'total_return_pct': total_return,
                'annualized_return': self._calculate_annualized_return(total_return, years),
                'avg_daily_return': period_data['Daily_Return'].mean(),
                'return_std': period_data['Daily_Return'].std(),
                'max_daily_loss': period_data['Daily_Return'].min(),
                'max_daily_gain': period_data['Daily_Return'].max(),
                'sharpe_ratio': self._calculate_sharpe_ratio(period_data['Daily_Return']),
                'max_drawdown': self._calculate_max_drawdown(period_data['Portfolio_Value'])
            }

            return results

        except Exception as e:
            return {"error": f"运行回测时出错: {str(e)}"}

    def _calculate_sharpe_ratio(self, returns, risk_free_rate=0.02):
        excess_returns = returns / 100 - risk_free_rate / 252
        if excess_returns.std() == 0:
            return 0
        return np.sqrt(252) * excess_returns.mean() / excess_returns.std()

    def _calculate_max_drawdown(self, portfolio_values):
        rolling_max = portfolio_values.expanding().max()
        drawdowns = (portfolio_values - rolling_max) / rolling_max * 100
        return drawdowns.min()


def format_results(results):
    if "error" in results:
        return f"错误: {results['error']}"

    output = f"""
回测结果报告:
====================
基本信息:
- 开始日期: {results['start_date']}
- 结束日期: {results['end_date']}
- 初始资金: ${results['initial_capital']:,.2f}
- 最终资金: ${results['final_capital']:,.2f}

交易统计:
- 交易天数: {results['trading_days']}天
- 总收益率: {results['total_return_pct']:.2f}%
- 年化收益率: {results['annualized_return']:.2f}%
- 平均日收益率: {results['avg_daily_return']:.2f}%
- 收益率标准差: {results['return_std']:.2f}%

风险指标:
- 最大日跌幅: {results['max_daily_loss']:.2f}%
- 最大日涨幅: {results['max_daily_gain']:.2f}%
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
            start_date='2024-02-11',  # 使用数据的实际开始日期
            end_date='2025-01-02',
            initial_capital=10000
        )

        print(format_results(results))

    except Exception as e:
        print(f"程序执行错误: {str(e)}")


