'''
- API 키 설정
'''
class Config:
    
    # Chat Completions 호출 경로
    MODEL_NAME = 'HCX-005'
    CHAT_COMPLETIONS_API = f'https://clovastudio.stream.ntruss.com/testapp/v3/chat-completions/{MODEL_NAME}'

    # CLOVA Studio API 인증 정보 (테스트 API 키)
    API_KEY = ''