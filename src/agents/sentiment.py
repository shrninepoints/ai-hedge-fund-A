from langchain_core.messages import HumanMessage
from src.agents.state import AgentState, show_agent_reasoning, show_workflow_status
from src.tools.news_crawler import get_stock_news, get_news_sentiment
from src.utils.logging_config import setup_logger
from src.utils.api_utils import agent_endpoint, log_llm_interaction
import json
from datetime import datetime, timedelta

# 设置日志记录
logger = setup_logger('sentiment_agent')


@agent_endpoint("sentiment", "情感分析师，分析市场新闻和社交媒体情绪")
def sentiment_agent(state: AgentState):
    """Responsible for sentiment analysis"""
    show_workflow_status("Sentiment Analyst")
    show_reasoning = state["metadata"]["show_reasoning"]
    data = state["data"]
    symbol = data["ticker"]
    logger.info(f"正在分析股票: {symbol}")
    # 从命令行参数获取新闻数量，默认为5条
    num_of_news = data.get("num_of_news", 10)

    # 获取新闻数据并分析情感
    news_list = get_stock_news(symbol, max_news=num_of_news)  # 确保获取足够的新闻

    # 过滤7天内的新闻
    cutoff_date = datetime.now() - timedelta(days=7)
    recent_news = [news for news in news_list
                   if datetime.strptime(news['publish_time'], '%Y-%m-%d %H:%M:%S') > cutoff_date]

    sentiment_score = get_news_sentiment(recent_news, num_of_news=num_of_news)

    # 根据情感分数生成交易信号和置信度
    if sentiment_score >= 0.5:
        signal = "bullish"
        confidence = str(round(abs(sentiment_score) * 100)) + "%"
    elif sentiment_score <= -0.5:
        signal = "bearish"
        confidence = str(round(abs(sentiment_score) * 100)) + "%"
    else:
        signal = "neutral"
        confidence = str(round((1 - abs(sentiment_score)) * 100)) + "%"

    # 生成分析结果
    message_content = {
        "signal": signal,
        "confidence": confidence,
        "reasoning": f"Based on {len(recent_news)} recent news articles, sentiment score: {sentiment_score:.2f}"
    }

    # 如果需要显示推理过程
    if show_reasoning:
        show_agent_reasoning(message_content, "Sentiment Analysis Agent")
        # 保存推理信息到metadata供API使用
        state["metadata"]["agent_reasoning"] = message_content

    # 创建消息
    message = HumanMessage(
        content=json.dumps(message_content),
        name="sentiment_agent",
    )

    show_workflow_status("Sentiment Analyst", "completed")
    # logger.info(
    # f"--- DEBUG: sentiment_agent RETURN messages: {[msg.name for msg in [message]]} ---")
    return {
        "messages": [message],
        "data": {
            **data,
            "sentiment_analysis": sentiment_score
        },
        "metadata": state["metadata"],
    }
