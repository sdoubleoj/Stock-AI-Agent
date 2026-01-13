'''
- LLM에게 전달할 Tool(함수) 목록을 정의하고, 
- 사용자의 질문과 함께 API를 호출하여 LLM의 응답(함수 호출 또는 텍스트)을 파싱하는 역할
'''
import requests
import json
from config import Config

# --- LLM에게 제공할 Tool 명세 정의 ---
# 각 함수의 역할과 필요한 파라미터를 상세히 설명해야 LLM이 제대로 사용함
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_stock_metric",
            "description": "특정 날짜의 특정 주식 종목에 대한 지정된 지표(시가, 고가, 저가, 종가, 거래량, 등락률)를 가져옵니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stock_name": {"type": "string", "description": "조회할 주식의 한글 이름 (예: 삼성전자)"},
                    "metric": {"type": "string", "enum": ["시가", "고가", "저가", "종가", "거래량", "등락률"], "description": "조회할 지표"},
                },
                "required": ["date", "stock_name", "metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_market_index",
            "description": "특정 날짜의 KOSPI 또는 KOSDAQ 시장 지수를 가져옵니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ"], "description": "조회할 시장"},
                },
                "required": ["date", "market"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_stocks_by_metric",
            "description": "지정된 날짜와 시장에서 특정 지표(거래량, 가격, 상승률, 하락률)를 기준으로 상위 N개 주식 종목을 가져옵니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ"], "description": "조회할 시장"},
                    "metric": {"type": "string", "enum": ["거래량", "가격", "상승률", "하락률"], "description": "순위를 매길 기준 지표"},
                    "n": {"type": "integer", "description": "가져올 상위 종목의 개수 (기본값: 5)"}
                },
                "required": ["date", "market", "metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_market_statistics",
            "description": "특정 날짜의 시장 통계를 조회합니다 (상승/하락 종목 수, 전체 거래대금, 시장별 거래된 종목 수 등).",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stat_type": {
                        "type": "string", 
                        "enum": ["rising_count", "falling_count", "total_trading_value", "market_rising_count", "market_traded_count"], 
                        "description": "조회할 통계 유형"
                    },
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ"], "description": "특정 시장 통계 조회 시 시장 구분 (market_rising_count, market_traded_count용)"}
                },
                "required": ["date", "stat_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_all_market_volume_ranking",
            "description": "전체 시장(KOSPI + KOSDAQ)에서 거래량 기준 상위 N개 종목을 조회합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "n": {"type": "integer", "description": "가져올 상위 종목의 개수 (기본값: 10)"}
                },
                "required": ["date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_volume_stock_with_count",
            "description": "특정 시장에서 거래량 1위 종목과 거래량 수치를 함께 조회합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ"], "description": "조회할 시장"}
                },
                "required": ["date", "market"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ask_for_clarification",
            "description": "모호하거나 불완전한 질문에 대해 구체적인 정보를 요청합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question_type": {
                        "type": "string", 
                        "enum": ["recent_rising_stocks", "stocks_down_from_high", "general_inquiry"], 
                        "description": "질문의 유형"
                    },
                    "missing_info": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["date", "market", "period", "count", "criteria"]},
                        "description": "부족한 정보 목록"
                    }
                },
                "required": ["question_type", "missing_info"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_rising_stocks",
            "description": "최근 상승한 주식들을 조회합니다. 모호한 질문에 대해 기본값을 사용하여 답변을 제공합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식 (기본값: 최근 거래일)"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ", "ALL"], "description": "조회할 시장 (기본값: ALL)"},
                    "n": {"type": "integer", "description": "가져올 상위 종목의 개수 (기본값: 5)"},
                    "period_days": {"type": "integer", "description": "조회 기간 (일 단위, 기본값: 1)"}
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stocks_down_from_high",
            "description": "52주 고점 대비 하락률이 큰 주식들을 조회합니다. '고점 대비 많이 떨어진 주식' 같은 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식 (기본값: 최근 거래일)"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ", "ALL"], "description": "조회할 시장 (기본값: ALL)"},
                    "n": {"type": "integer", "description": "가져올 상위 종목의 개수 (기본값: 5)"},
                    "weeks": {"type": "integer", "description": "고점을 계산할 기간 (주 단위, 기본값: 52주)"}
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_stocks",
            "description": "두 종목의 특정 지표를 비교하여 더 높은/낮은 종목을 반환합니다. '카카오와 현대차 중 종가가 더 높은 종목은?' 같은 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stock1": {"type": "string", "description": "비교할 첫 번째 종목명"},
                    "stock2": {"type": "string", "description": "비교할 두 번째 종목명"},
                    "metric": {"type": "string", "enum": ["종가", "시가", "고가", "저가", "등락률", "거래량"], "description": "비교할 지표"},
                    "comparison": {"type": "string", "enum": ["higher", "lower"], "description": "비교 방향 (기본값: higher)"}
                },
                "required": ["date", "stock1", "stock2", "metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_market_indices",
            "description": "KOSPI와 KOSDAQ 지수를 비교합니다. 'KOSPI와 KOSDAQ 중 더 높은 지수는?' 같은 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "comparison": {"type": "string", "enum": ["higher", "lower"], "description": "비교 방향 (기본값: higher)"}
                },
                "required": ["date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculate_market_average_change",
            "description": "특정 시장의 평균 등락률을 계산합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ"], "description": "조회할 시장 (기본값: KOSPI)"}
                },
                "required": ["date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_stock_to_market",
            "description": "특정 종목의 등락률을 시장 평균과 비교합니다. '셀트리온의 등락률이 시장 평균보다 높은가?' 같은 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stock_name": {"type": "string", "description": "비교할 종목명"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ"], "description": "비교할 시장 (기본값: KOSPI)"}
                },
                "required": ["date", "stock_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculate_stock_volume_share",
            "description": "특정 종목의 거래량이 전체 시장 거래량에서 차지하는 비율을 계산합니다. 'SK하이닉스의 거래량이 전체 시장 거래량의 몇 %인가?' 같은 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stock_name": {"type": "string", "description": "조회할 종목명"}
                },
                "required": ["date", "stock_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_volume_rank",
            "description": "특정 종목의 전체 시장에서의 거래량 순위를 조회합니다. '셀트리온의 거래량 순위는?' 같은 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stock_name": {"type": "string", "description": "순위를 조회할 종목명"},
                    "market": {"type": "string", "enum": ["KOSPI", "KOSDAQ", "ALL"], "description": "조회할 시장 범위 (기본값: ALL)"}
                },
                "required": ["date", "stock_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculate_market_cap",
            "description": "특정 종목의 시가총액을 계산합니다. 시가총액 = 종가 × 상장주식수로 계산됩니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stock_name": {"type": "string", "description": "시가총액을 계산할 종목명"}
                },
                "required": ["date", "stock_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_market_caps",
            "description": "두 종목의 시가총액을 비교합니다. '카카오와 LG화학 중 시가총액이 더 큰 종목은?' 같은 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "조회할 날짜, 'YYYY-MM-DD' 형식"},
                    "stock1": {"type": "string", "description": "비교할 첫 번째 종목명"},
                    "stock2": {"type": "string", "description": "비교할 두 번째 종목명"},
                    "comparison": {"type": "string", "enum": ["higher", "lower"], "description": "비교 방향 (기본값: higher)"}
                },
                "required": ["date", "stock1", "stock2"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_by_technical_signal",
            "description": "시그널 분석 조건(볼린저 밴드, RSI, 골든/데드크로스 등)을 만족하는 종목을 필터링합니다.",
            "parameters": {
            "type": "object",
            "properties": {
                "question": {
                "type": "string",
                "description": "자연어 질문 (예: '2025년 3월 5일에 볼린저 밴드 하단에 닿은 종목 알려줘')"
                }
            },
            "required": ["question"]
            }
        }
    }
]


# --- LLM API 호출 및 응답 처리 ---
def get_llm_function_call(user_query, chat_history=None):
    '''
    - 사용자 질문과 Tool 목록을 LLM API에 보내고, 그 응답을 반환
    '''
    url = Config.CHAT_COMPLETIONS_API
    headers = {
        'Authorization': f'Bearer {Config.API_KEY}',
        # 'X-NCP-CLOVASTUDIO-REQUEST-ID': f'{Config.REQUEST_ID_CHAT}',
        'Content-Type': 'application/json; charset=utf-8',
    }
    
    messages = []
    
    # 2차 호출(tool 실행 결과를 바탕으로 최종 답변 생성)일 경우, 간결한 답변을 위한 시스템 프롬프트 추가
    is_second_call = user_query is None and chat_history and chat_history[-1]['role'] == 'tool'
    if is_second_call:
        messages.append({
            "role": "system",
            "content": "Tool이 결과를 반환하면, 추가적인 설명, 분석, 조언, 또는 불필요한 문장 없이 결과의 핵심 정보만 간결하게 한 문장으로 전달하세요. 예를 들어, Tool이 '786.29'를 반환하면 'KOSDAQ 지수는 786.29입니다.' 와 같이 핵심만 답변하고 말을 끝내세요. Tool이 '데이터가 없습니다' 류의 결과를 반환하면 그대로 전달하세요."
        })
    else:
        # 첫 번째 호출 시 모호한 질문 처리를 위한 시스템 프롬프트 추가
        messages.append({
            "role": "system",
            "content": """당신은 금융 정보 전문 AI 에이전트입니다. 다음 원칙을 따라 답변하세요:

1. **날짜 관련 질문 처리** (매우 중요):
   - 질문에 특정 날짜가 포함되어 있으면 항상 해당 Tool을 호출하여 실제 데이터를 확인하세요
   - 미래/과거 날짜 구분 없이 모든 날짜에 대해 실제 데이터 조회를 시도하세요
   - "미래 예측 불가능", "미래 데이터 없음" 같은 답변은 절대 하지 마세요
   - 대신 Tool을 호출해서 해당 날짜의 데이터 존재 여부를 확인하세요
   - Tool 호출 결과가 "데이터 없음"일 경우에만 "해당 날짜의 데이터가 없습니다"라고 답변하세요

2. **정확한 정보가 있는 경우**: 적절한 Tool을 사용하여 정확한 데이터를 제공하세요.

3. **비교 및 순위 질문 처리**: 다음과 같은 질문들을 적절한 함수로 처리하세요:
   - "A와 B 중 종가가 더 높은 종목은?" → compare_stocks 사용
   - "A와 B 중 시가총액이 더 큰 종목은?" → compare_market_caps 사용
   - "KOSPI와 KOSDAQ 중 더 높은 지수는?" → compare_market_indices 사용
   - "A의 등락률이 시장 평균보다 높은가?" → compare_stock_to_market 사용
   - "A의 거래량이 전체 시장 거래량의 몇 %인가?" → calculate_stock_volume_share 사용
   - "A의 거래량 순위는?" → get_stock_volume_rank 사용

4. **모호한 질문 처리**: 다음과 같은 모호한 표현이 있을 때는 적절히 처리하세요:
   - "최근", "요즘" → 구체적인 날짜가 없으면 기본값(최근 거래일) 사용 또는 ask_for_clarification 호출
   - "많이 오른", "급등한" → 상승률 기준 상위 종목으로 해석, get_recent_rising_stocks 사용
   - "고점 대비 하락한", "많이 떨어진" → 52주 고점 대비 하락률로 해석, get_stocks_down_from_high 사용
   - 시장이 명시되지 않은 경우 → 전체 시장(KOSPI+KOSDAQ) 또는 되묻기
   - 개수가 명시되지 않은 경우 → 기본값 5개 사용

5. **되묻기 시점**: 다음 경우에 ask_for_clarification 사용:
   - 핵심 정보가 완전히 빠진 경우
   - 답변의 정확성을 위해 명확화가 필요한 경우
   - 구현되지 않은 복잡한 분석이 요구되는 경우

6. **기본값 활용**: 간단한 정보 부족 시에는 합리적인 기본값을 사용하여 get_recent_rising_stocks 등을 활용하세요."""
        })

    if chat_history:
        messages.extend(chat_history)
    if user_query:
        messages.append({'role': 'user', 'content': user_query})

    payload = {
        "messages": messages,
        "tools": TOOLS,
        "toolChoice": "auto", # LLM이 함수 사용 여부를 자율적으로 결정
        "temperature": 0.1,
        "max_tokens": 1024,
        "seed": 42,
    }

    try:
        if not url:
            raise ValueError("API URL이 config.py에 설정되지 않았습니다.")
        
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        return response.json() # LLM의 응답을 그대로 반환

    except Exception as e:
        print(f"API 호출 오류: {e}")
        # 오류 발생 시, 사용자에게 보여줄 대체 메시지 생성
        return {
            "result": {
                "message": {
                    "role": "assistant",
                    "content": f"API 호출에 실패했습니다. (오류: {e})",
                }
            }
        } 