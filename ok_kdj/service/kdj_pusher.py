import json
import os
import pandas as pd
import requests
import logging
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class KDJData:
    """KDJ数据结构"""
    symbol: str
    timestamp: str
    k_value: float
    d_value: float
    j_value: float
    price: float = 0.0


class KDJPushBot:
    """KDJ推送机器人"""

    def __init__(self, webhook_url: str = "", push_token: Optional[str] = None):
        self.webhook_url = webhook_url or "https://oapi.dingtalk.com/robot/send?access_token=6880ca1bf3410a1937dd80a3648b9e1792344bca36898d2d9919f76d1c826465"
        self.push_token = push_token
        self.conditions = [
            {"name": "条件2", "condition": lambda k, j: k > 90 and j < 105},
            {"name": "条件3", "condition": lambda k, j: k > 85 and j > 105},
            {"name": "条件4", "condition": lambda k, j: k < 20 and j < -10},
            {"name": "条件5", "condition": lambda k, j: k < 15 and j < -5}
        ]

    def check_conditions(self, kdj_data: KDJData) -> List[str]:
        """检查KDJ是否满足条件"""
        matched_conditions = []
        for condition in self.conditions:
            if condition["condition"](kdj_data.k_value, kdj_data.j_value):
                matched_conditions.append(condition["name"])
        return matched_conditions

    def format_message(self, kdj_data: KDJData, conditions: List[str]) -> str:
        """格式化推送消息"""
        message = f"""
📊 KDJ信号提醒
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔖 股票代码: {kdj_data.symbol}
⏰ 时间: {kdj_data.timestamp}
💰 价格: {kdj_data.price}

📈 KDJ指标:
  K值: {kdj_data.k_value:.2f}
  D值: {kdj_data.d_value:.2f}
  J值: {kdj_data.j_value:.2f}

🎯 触发条件: {', '.join(conditions)}

📋 条件说明:
• 条件2: K>90 且 J<105 (超买区间)
• 条件3: K>85 且 J>105 (强势突破)
• 条件4: K<20 且 J<-10 (超卖区间)
• 条件5: K<15 且 J<-5 (深度超卖)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """.strip()
        return message

    def send_push_notification(self, message: str) -> bool:
        """发送通知到钉钉/其他Webhook"""
        if not self.webhook_url:
            logger.warning("Webhook地址未配置，将打印消息：")
            print(message)
            return True

        headers = {"Content-Type": "application/json"}
        if self.push_token:
            headers["Authorization"] = f"Bearer {self.push_token}"

        payload = {
            "msgtype": "text",
            "text": {"content": message}
        }

        try:
            response = requests.post(self.webhook_url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                logger.info("✅ 推送成功")
                return True
            else:
                logger.error(f"❌ 推送失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ 推送时发生错误: {e}")
            return False

    def process_file(self, file_path: str) -> int:
        """
        处理CSV文件，仅处理最后一行的KDJ数据

        Args:
            file_path: 文件路径（仅支持CSV）

        Returns:
            推送成功数量（最多为1）
        """
        logger.info(f"开始处理文件: {file_path}")
        symbol = os.path.basename(os.path.dirname(file_path))
        try:
            if not file_path.endswith('.csv'):
                logger.warning("当前只支持CSV文件")
                return 0

            df = pd.read_csv(file_path)
            if df.empty:
                logger.warning("CSV为空")
                return 0

            row = df.iloc[-2]

            # 支持有/无列名
            if 'symbol' in df.columns:
                kdj = KDJData(
                    symbol=symbol,
                    timestamp=row.get('timestamp', ''),
                    k_value=float(row.get('k', 0)),
                    d_value=float(row.get('d', 0)),
                    j_value=float(row.get('j', 0)),
                    price=float(row.get('price', 0))
                )
            else:
                kdj = KDJData(
                    symbol=symbol,
                    timestamp=str(row[0]),
                    k_value=float(row[7]),
                    d_value=float(row[8]),
                    j_value=float(row[9]),
                    price=float(row[5]) if len(row) > 5 else 0.0
                )

            matched = self.check_conditions(kdj)
            if matched:
                message = self.format_message(kdj, matched)
                if self.send_push_notification(message):
                    logger.info(f"✅ 已推送: {kdj.timestamp} K={kdj.k_value:.2f}, J={kdj.j_value:.2f}")
                    return 1
                else:
                    logger.warning("❌ 推送失败")
            else:
                logger.info("⏸ 最新数据未触发条件")
            return 0

        except Exception as e:
            logger.error(f"处理文件失败: {e}")
            return 0


def main():
    bot = KDJPushBot()
    file_path = "kdj_data.csv"  # 替换成你的文件路径
    bot.process_file(file_path)


if __name__ == "__main__":
    main()
