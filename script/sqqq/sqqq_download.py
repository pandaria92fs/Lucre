import requests
import pandas as pd
from datetime import datetime


def get_sqqq_data(api_key):
    """
    使用Alpha Vantage API下载SQQQ数据

    参数:
    api_key: Alpha Vantage的免费API key

    返回:
    pandas DataFrame包含历史数据
    """
    # Alpha Vantage API endpoint
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SQQQ&outputsize=full&apikey={api_key}'

    try:
        # 发送请求
        response = requests.get(url)
        data = response.json()

        # 提取时间序列数据
        time_series = data.get('Time Series (Daily)', {})

        # 转换为DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')

        # 重命名列
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # 转换数据类型
        for col in df.columns:
            df[col] = pd.to_numeric(df[col])

        # 添加日期列
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

        # 计算日收益率
        df['Daily_Return'] = df['Close'].pct_change() * 100

        # 计算累计收益率
        df['Cumulative_Return'] = (1 + df['Daily_Return'] / 100).cumprod() - 1
        df['Cumulative_Return'] = df['Cumulative_Return'] * 100

        return df

    except Exception as e:
        print(f"获取数据时出错: {e}")
        return None


def analyze_data(df):
    """
    分析SQQQ数据
    """
    if df is None:
        return

    # 基本统计
    analysis = {
        '开始日期': df.index[0].strftime('%Y-%m-%d'),
        '结束日期': df.index[-1].strftime('%Y-%m-%d'),
        '交易天数': len(df),
        '最新收盘价': df['Close'].iloc[-1],
        '平均日收益率': df['Daily_Return'].mean(),
        '收益率标准差': df['Daily_Return'].std(),
        '最大日涨幅': df['Daily_Return'].max(),
        '最大日跌幅': df['Daily_Return'].min(),
        '总收益率': df['Cumulative_Return'].iloc[-1]
    }

    return analysis


# 使用示例
if __name__ == "__main__":
    # 需要先获取免费的API key: https://www.alphavantage.co/support/#api-key
    api_key = '1UAIH4AQ8QWAJ0W3'  # 替换成你的API key

    # 下载数据
    df = get_sqqq_data(api_key)

    if df is not None:
        # 保存到CSV
        df.to_csv('sqqq_data.csv')

        # 打印分析结果
        analysis = analyze_data(df)
        for key, value in analysis.items():
            print(f"{key}: {value}")


def calculate_drawdown(df):
    """
    计算最大回撤
    """
    if df is None:
        return None

    # 计算滚动最大值
    rolling_max = df['Close'].expanding().max()
    drawdown = (df['Close'] - rolling_max) / rolling_max * 100

    return {
        '最大回撤': drawdown.min(),
        '当前回撤': drawdown.iloc[-1]
    }

