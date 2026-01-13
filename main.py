'''
- 터미널 환경에서 금융 정보 에이전트를 실행하는 메인 스크립트 (Function Calling 방식)
- 동작 방식
    1. 사용자가 질문을 입력
    2. function_caller.py의 get_llm_function_call을 호출하여, LLM이 사용자의 질문을 분석하고 어떤 도구(함수)를 사용할지 결정
    3. 만약 LLM이 함수 사용을 결정하면, skillset.py에 정의된 해당 함수(예: get_stock_metric)를 실행하여 데이터를 가져옴
    4. 가져온 데이터를 바탕으로 다시 get_llm_function_call을 호출하여 최종 사용자 답변을 생성
'''
import json
import time
from function_caller import get_llm_function_call
from skillset import SKILL_HANDLERS

def main():
    initial_message = '하이~ 나는 금융 AI 에이전트 정비스🤖다.\n'
    print(initial_message)

    chat_history = []

    while True:
        query = input("질문: ")
        if query.lower() in ["exit", "quit"]:
            print("🤖: 바이바이")
            break
        
        start_time = time.time()

        # 현재 턴의 메시지 기록 (대화 기록과 별도 관리)
        current_messages = [{"role": "user", "content": query}]
        
        # 1. 사용자 질문을 LLM에게 보내 함수 호출 정보를 얻음
        # 이전 대화 기록(chat_history)을 함께 전달하여 맥락 유지
        llm_response = get_llm_function_call(None, chat_history + current_messages)
        
        message = llm_response.get("result", {}).get("message", {})
        
        # LLM의 응답(tool_calls 포함 가능)을 현재 턴의 기록에 추가
        current_messages.append(message)
        final_answer = ""
        
        # 2. LLM이 함수 호출을 결정했는지 확인
        if message.get("toolCalls"):
            tool_call = message["toolCalls"][0]
            function_name = tool_call["function"]["name"]
            function_args = tool_call["function"]["arguments"]
            tool_call_id = tool_call["id"]

            if function_name in SKILL_HANDLERS:
                handler = SKILL_HANDLERS[function_name]
                try:
                    function_result = handler(**function_args)
                    
                    # 함수 실행 결과를 현재 턴의 기록에 추가
                    current_messages.append({
                        "role": "tool",
                        "content": str(function_result), # 결과를 문자열로 변환
                        "toolCallId": tool_call_id
                    })
                    
                    # 3. 전체 대화 흐름(이전 기록 + 현재 턴)을 포함하여 다시 LLM을 호출
                    second_response = get_llm_function_call(None, chat_history + current_messages)
                    final_answer = second_response.get("result", {}).get("message", {}).get("content", "최종 답변 생성에 실패")
                    
                except Exception as e:
                    final_answer = f"Function Execution Error: {function_name} 실행 중 오류 발생: {e}"
            else:
                final_answer = f"Error: LLM이 알 수 없는 함수({function_name})를 호출"
        else:
            final_answer = message.get("content", "응답을 생성하지 못했습니다.")

        print(f"🤖: {final_answer}\n")
        
        # 전체 대화 기록에 현재 턴의 사용자 질문과 최종 답변만 추가
        chat_history.append({"role": "user", "content": query})
        chat_history.append({"role": "assistant", "content": final_answer})


if __name__ == "__main__":
    main() 
    
    
'''
실행 예시
python main.py
'''
