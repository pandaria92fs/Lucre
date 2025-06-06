import requests
import json
from typing import Dict, Any, Optional, Set


def get_okx_perpetual_swaps() -> Optional[Dict[str, Any]]:
    """
    获取OKX永续合约列表

    Returns:
        Dict: API返回的永续合约数据，失败时返回None
    """
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ 获取永续合约数据失败: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ JSON解析失败: {e}")
        return None


import pandas as pd
import json


def count_perpetual_currencies() -> int:
    """
    统计永续合约支持的币种数量并保存到CSV文件

    Returns:
        int: 永续合约支持的币种数量
    """
    print("⚡ 正在获取OKX永续合约数据...")

    swap_data = get_okx_perpetual_swaps()

    if not swap_data or swap_data.get('code') != '0':
        print("❌ 获取永续合约数据失败")
        return 0

    instruments = swap_data.get('data', [])
    currencies = set()

    print(f"📊 永续合约总数: {len(instruments)}")
    print("🔍 数据样例:")
    if instruments:
        print(json.dumps(instruments[0], indent=2, ensure_ascii=False))
        print(json.dumps(instruments[1], indent=2, ensure_ascii=False))
        print(json.dumps(instruments[2], indent=2, ensure_ascii=False))

    # 分析不同类型的永续合约
    inverse_count = 0
    linear_count = 0
    settleCcy_coins = set()
    baseCcy_coins = set()
    instId_coins = set()

    # 存储instId列表
    instid_list = []

    for instrument in instruments:
        ct_type = instrument.get('ctType', '')

        if ct_type == 'inverse':
            inverse_count += 1
        elif ct_type == 'linear':
            linear_count += 1

        if 'instId' in instrument:
            inst_id = instrument['instId']
            if '-' in inst_id and "SWAP" in inst_id:
                coin_name = inst_id
                instId_coins.add(coin_name)
                currencies.add(coin_name)
                instid_list.append(inst_id)

    print(f"\n📈 合约类型统计:")
    print(f"   反向合约(inverse): {inverse_count} 个")
    print(f"   线性合约(linear): {linear_count} 个")
    print(f"   其他类型: {len(instruments) - inverse_count - linear_count} 个")

    print(f"\n🔍 币种来源统计:")
    print(f"   从settleCcy提取: {len(settleCcy_coins)} 个币种")
    print(f"   从baseCcy提取: {len(baseCcy_coins)} 个币种")
    print(f"   从instId提取: {len(instId_coins)} 个币种")

    # 显示结果
    print(f"\n🎯 OKX永续合约支持的币种数量: {len(currencies)} 个")
    print(f"📊 预期数量: 271 个，差异: {271 - len(currencies)}")

    # 显示所有币种
    sorted_currencies = sorted(list(currencies))
    print(f"📋 支持永续合约的币种列表:")
    print(f"   {', '.join(currencies)}")

    # 使用pandas保存到CSV文件
    try:
        df = pd.DataFrame({'instId': sorted(instid_list)})
        df.to_csv('swap.csv', index=False, encoding='utf-8')

        print(f"\n💾 数据已保存到CSV文件: swap.csv")
        print(f"📊 共写入 {len(instid_list)} 条instId记录")

    except Exception as e:
        print(f"❌ 保存CSV文件时出错: {str(e)}")

    return len(currencies)


if __name__ == "__main__":

        count_perpetual_currencies()