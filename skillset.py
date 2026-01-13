'''
- 스킬셋: 실제 금융 데이터를 조회하는 함수(Tool)들의 모음
- 각 함수는 명확한 단일 작업을 수행하며, function_caller에 의해 호출됨
'''
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import warnings
import logging
import sys
import os
import FinanceDataReader as fdr
import json
import re
from langchain_naver import ChatClovaX
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# yfinance 경고 및 오류 메시지 억제
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# --- Setup from v0.2 ---
load_dotenv()
llm = ChatClovaX(model="HCX-005", temperature=0.3, top_p=0.8, max_tokens=256)

# stdout 캡처를 위한 클래스
class SuppressOutput:
    def __enter__(self):
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr

# --- 전역 변수 및 캐시 ---
# 1. 빠른 조회를 위한 기본 종목 맵 (캐시 역할)
STOCK_TICKER_MAP = {
    # KOSPI 주요 종목
    "삼성전자": "005930.KS", "SK하이닉스": "000660.KS", "현대차": "005380.KS",
    "NAVER": "035420.KS", "LG화학": "051910.KS", "삼성SDI": "006400.KS",
    "삼성바이오로직스": "207940.KS", "KCTC": "009070.KS", "동방": "004140.KS",
    "콜마홀딩스": "024720.KS", "한국전력": "015760.KS", "두산에너빌리티": "034020.KS",
    "롯데지주": "004990.KS", "CS홀딩스": "000590.KS", "대한방직": "001070.KS",
    "성신양회": "004980.KS", "삼정펄프": "009770.KS", "롯데케미칼": "011170.KS",
    "삼영엠텍": "054540.KS", "카카오": "035720.KS", "셀트리온": "068270.KS",
    "삼성바이오로직스": "207940.KS", "하이트진로2우B": "000087.KS",
    
    # 추가 KOSPI 종목들
    "포스코홀딩스": "005490.KS", "LG전자": "066570.KS", "한국금융지주": "071050.KS",
    "신한지주": "055550.KS", "KB금융": "105560.KS", "하나금융지주": "086790.KS",
    "현대글로비스": "086280.KS", "아모레퍼시픽": "090430.KS", "기아": "000270.KS",
    "현대모비스": "012330.KS", "LG디스플레이": "034220.KS", "SK이노베이션": "096770.KS",
    "우리금융지주": "316140.KS", "삼성물산": "028260.KS", "POSCO DX": "022100.KS",
    
    # KOSDAQ 주요 종목
    "카카오페이": "377300.KQ", "제주은행": "006220.KQ", "형지엘리트": "093240.KQ",
    "대성미생물": "036480.KQ", "도화엔지니어링": "002150.KQ", "KG이니시스": "035600.KQ",
    "케이씨에스": "115500.KQ", "알테오젠": "196170.KQ", "에스비비테크": "389500.KQ",
    "세화피앤씨": "252500.KQ", "퓨릿": "445180.KQ", "버넥트": "438700.KQ",
    "한양증권": "001750.KQ", "우진비앤지": "018620.KQ", "한네트": "052600.KQ",
    "코아스템켐온": "166480.KQ", "에코프로": "086520.KQ", "에코프로비엠": "247540.KQ",
    
    # 추가 KOSDAQ 종목들
    "카카오게임즈": "293490.KQ", "펄어비스": "263750.KQ", "위메이드": "112040.KQ",
    "휴젤": "145020.KQ", "셀트리온제약": "068760.KQ", "셀트리온헬스케어": "091990.KQ",
    "메디톡스": "086900.KQ", "클래시스": "214150.KQ", "엔씨소프트": "036570.KQ",
    "넷마블": "251270.KQ", "크래프톤": "259960.KQ", "에이치엘비": "028300.KQ",
    "카카오뱅크": "323410.KQ", "라이프시멘틱스": "089970.KQ", "한국콜마": "161890.KQ"
}
KOSPI_TICKERS = list(v for k, v in STOCK_TICKER_MAP.items() if ".KS" in v)
KOSDAQ_TICKERS = list(v for k, v in STOCK_TICKER_MAP.items() if ".KQ" in v)
MARKET_INDEX_TICKERS = {"KOSPI": "^KS11", "KOSDAQ": "^KQ11"}

# 2. 전체 종목 티커 캐시 (pykrx 동적 조회)
_KRX_TICKER_CACHE = None
_FDR_KRX_CACHE = None

def _initialize_krx_cache():
    '''
    - KRX로부터 전체 종목의 티커와 이름을 조회하여 캐시를 초기화
    '''
    global _KRX_TICKER_CACHE
    if _KRX_TICKER_CACHE is None:
        _KRX_TICKER_CACHE = {}
        try:
            today_str = datetime.now().strftime("%Y%m%d")
            for market_code in ["KOSPI", "KOSDAQ"]:
                tickers = stock.get_market_ticker_list(today_str, market=market_code)
                for ticker in tickers:
                    name = stock.get_market_ticker_name(ticker)
                    # yfinance 형식에 맞게 접미사 추가
                    suffix = ".KS" if market_code == "KOSPI" else ".KQ"
                    _KRX_TICKER_CACHE[name] = f"{ticker}{suffix}"
        except Exception as e:

def get_krx_cache():
    global _FDR_KRX_CACHE
    if _FDR_KRX_CACHE is None:
        _FDR_KRX_CACHE = fdr.StockListing("KRX")
    return _FDR_KRX_CACHE

def _get_all_market_tickers(market=None):
    '''
    - 지정된 시장(또는 전체 시장)의 모든 티커를 반환
    '''
    _initialize_krx_cache()
    
    if market == "KOSPI":
        return [ticker for ticker in _KRX_TICKER_CACHE.values() if ".KS" in ticker]
    elif market == "KOSDAQ":
        return [ticker for ticker in _KRX_TICKER_CACHE.values() if ".KQ" in ticker]
    else:
        return list(_KRX_TICKER_CACHE.values())

def _get_previous_trading_day(date_str=None):
    '''
    - 주어진 날짜(또는 오늘)의 이전 거래일을 반환
    '''
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        target_date = datetime.now()
    
    # 주말 체크하여 가장 최근 거래일 찾기
    while target_date.weekday() >= 5:  # 토요일(5), 일요일(6)
        target_date -= timedelta(days=1)
    
    return target_date.strftime("%Y-%m-%d")

# --- 헬퍼 함수 ---
def get_ticker(stock_name):
    '''
    - 종목명을 기반으로 티커를 찾음
    - 먼저 내부 맵(STOCK_TICKER_MAP)에서 찾고, 없으면 KRX 전체 목록을 조회
    '''
    # 1. 내부 맵(STOCK_TICKER_MAP)에서 먼저 검색
    ticker = STOCK_TICKER_MAP.get(stock_name)
    if ticker:
        return ticker

    # 2. 내부 맵에 없으면, KRX 전체 캐시에서 검색
    _initialize_krx_cache() # 캐시가 비어있으면 초기화
    ticker = _KRX_TICKER_CACHE.get(stock_name)
    if ticker:
        # 찾은 종목을 다음 빠른 조회를 위해 내부 맵에 추가
        STOCK_TICKER_MAP[stock_name] = ticker
        return ticker
        
    return None

# --- 주식 데이터 조회 (개선된 버전) ---
def get_history(ticker, date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        end_date_obj = date_obj + timedelta(days=1)
        
        # 오류 메시지 억제하면서 데이터 조회
        with SuppressOutput():
            stock = yf.Ticker(ticker)
            hist = stock.history(start=date_str, end=end_date_obj.strftime("%Y-%m-%d"))
        
        if hist.empty:
            return None
        return hist.iloc[0]
    except Exception:
        return None

def get_history_with_previous(ticker, date_str):
    '''
    - 특정 날짜와 이전 거래일의 데이터를 함께 가져옴 (등락률 계산용)
    '''
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        # 이전 거래일을 포함하기 위해 일주일 전부터 조회
        start_date_obj = date_obj - timedelta(days=7)
        end_date_obj = date_obj + timedelta(days=1)
        
        # 오류 메시지 억제하면서 데이터 조회
        with SuppressOutput():
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date_obj.strftime("%Y-%m-%d"), 
                               end=end_date_obj.strftime("%Y-%m-%d"))
        
        if hist.empty or len(hist) < 1:
            return None, None
            
        # 요청한 날짜의 데이터 찾기
        target_date = date_obj.strftime("%Y-%m-%d")
        target_data = None
        previous_data = None
        
        for i, (date_index, row) in enumerate(hist.iterrows()):
            if date_index.strftime("%Y-%m-%d") == target_date:
                target_data = row
                # 이전 거래일 데이터 (바로 전 행)
                if i > 0:
                    previous_data = hist.iloc[i-1]
                break
        
        return target_data, previous_data
    except Exception:
        return None, None

def _safe_yf_download(tickers, start_date, end_date):
    '''
    - yfinance download를 안전하게 실행하고 오류 메시지를 억제
    '''
    try:
        with SuppressOutput():
            data = yf.download(tickers, start=start_date, end=end_date, 
                             progress=False, group_by='ticker', auto_adjust=False)
        return data
    except Exception:
        return pd.DataFrame()

# --- 실제 작업을 수행하는 함수 (Tools) ---
# 1. 주식 데이터 조회 (등락률 계산 개선)
def get_stock_metric(**kwargs):
    '''
    - 특정 날짜의 특정 주식 종목에 대한 지정된 지표(metric)를 가져옴
    - 종목명을 기반으로 티커를 찾고, 티커를 기반으로 데이터를 조회
    - 조회된 데이터는 특정 날짜의 데이터를 반환
    '''
    date = kwargs.get('date')
    stock_name = kwargs.get('stock_name')
    metric = kwargs.get('metric')

    ticker = get_ticker(stock_name)
    if not ticker:
        return f"'{stock_name}'에 대한 티커 정보를 찾을 수 없습니다."

    # 등락률 계산을 위해서는 이전 거래일 데이터도 필요
    if metric == '등락률':
        target_data, previous_data = get_history_with_previous(ticker, date)
        if target_data is None:
            return f"{date}에 '{stock_name}'의 거래 데이터가 없습니다."
        if previous_data is None:
            return f"{date} '{stock_name}'의 전일 거래 데이터가 없어 등락률을 계산할 수 없습니다."
        
        # 정확한 등락률 계산: (오늘 종가 - 어제 종가) / 어제 종가 * 100
        today_close = target_data['Close']
        yesterday_close = previous_data['Close']
        
        if pd.isna(today_close) or pd.isna(yesterday_close) or yesterday_close == 0:
            return f"{date} '{stock_name}'의 등락률을 계산할 수 없습니다."
            
        change = (today_close - yesterday_close) / yesterday_close * 100
        return f"{change:+.2f}%"
    
    # 다른 지표들은 기존 방식 사용
    hist = get_history(ticker, date)
    if hist is None:
        return f"{date}에 '{stock_name}'의 거래 데이터가 없습니다."

    metric_map = {"시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume"}
    metric_key = metric_map.get(metric)

    if metric_key:
        if metric == "거래량":
            volume = int(hist[metric_key])
            return f"{volume:,}주"
        else:
            price = hist[metric_key]
            return f"{price:,.0f}원"
    
    return f"'{metric}'는 알 수 없는 지표입니다."

# 2. 시장 지수 조회
def get_market_index(**kwargs):
    '''
    - 특정 날짜의 KOSPI 또는 KOSDAQ 시장 지수를 가져옴
    - 시장 코드(KOSPI, KOSDAQ)를 기반으로 티커를 찾고, 티커를 기반으로 데이터를 조회
    - 조회된 데이터는 특정 날짜의 데이터를 반환
    '''
    date = kwargs.get('date')
    market = kwargs.get('market')
    
    ticker = MARKET_INDEX_TICKERS.get(market)
    if not ticker:
        return f"'{market}'는 지원하지 않는 시장입니다."

    hist = get_history(ticker, date)
    if hist is None:
        return f"{date}의 {market} 지수 데이터가 없습니다."
        
    return f"{hist['Close']:.2f}"

# 3. 상위 종목 조회 (개선된 버전)
def get_top_stocks_by_metric(**kwargs):
    '''
    - 지정된 날짜와 시장에서 특정 지표(거래량, 가격, 상승률, 하락률)를 기준으로 상위 N개 주식 종목을 가져옴
    '''
    date = kwargs.get('date')
    market = kwargs.get('market')
    metric = kwargs.get('metric')
    n = int(kwargs.get('n', 5))

    tickers = _get_all_market_tickers(market)
    if not tickers:
        return f"'{market}' 시장의 종목 정보를 가져올 수 없습니다."

    # 등락률 계산을 위해서는 더 긴 기간의 데이터가 필요
    if metric in ["상승률", "하락률"]:
        start_date_obj = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=7)
        start_date = start_date_obj.strftime("%Y-%m-%d")
    else:
        start_date = date
    
    end_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    # 안전한 데이터 다운로드로 개선
    data_chunks = []
    chunk_size = 50  # 청크 크기 축소로 안정성 향상
    
    for i in range(0, len(tickers), chunk_size):
        chunk_tickers = tickers[i:i+chunk_size]
        chunk_data = _safe_yf_download(chunk_tickers, start_date, end_date)
        if not chunk_data.empty:
            data_chunks.append(chunk_data)
            
    if not data_chunks:
        return f"{date}에 대한 데이터를 가져올 수 없습니다."
        
    # 데이터 병합
    data = pd.concat(data_chunks, axis=1) if len(data_chunks) > 1 else data_chunks[0]

    if data.empty:
        return f"{date}에 대한 데이터를 가져올 수 없습니다."
    
    # 각 티커별로 해당 날짜의 데이터 추출
    stock_data = []
    target_date = datetime.strptime(date, "%Y-%m-%d")
    
    for ticker in tickers:
        try:
            if ticker in data.columns.get_level_values(0):
                ticker_data = data[ticker]
                if not ticker_data.empty and not ticker_data.isna().all().all():
                    # 요청 날짜의 데이터 찾기
                    target_row = None
                    previous_row = None
                    
                    for i, (date_index, row) in enumerate(ticker_data.iterrows()):
                        if date_index.date() == target_date.date():
                            target_row = row
                            if i > 0:
                                previous_row = ticker_data.iloc[i-1]
                            break
                    
                    if target_row is not None and not target_row.isna().all():
                        stock_entry = {'ticker': ticker, **target_row.to_dict()}
                        
                        # 등락률 계산이 필요한 경우 이전일 데이터 추가
                        if metric in ["상승률", "하락률"] and previous_row is not None:
                            stock_entry['Previous_Close'] = previous_row['Close']
                        
                        stock_data.append(stock_entry)
        except:
            continue

    if not stock_data:
        return f"{date}에 거래 데이터가 있는 종목이 없습니다."
    
    df = pd.DataFrame(stock_data)
    df.set_index('ticker', inplace=True)
    
    # 종목명 매핑
    ticker_to_name_map = {v: k for k, v in _KRX_TICKER_CACHE.items()} if _KRX_TICKER_CACHE else {}
    df['stock_name'] = df.index.map(ticker_to_name_map)
    df.dropna(subset=['stock_name', 'Open', 'Close', 'Volume'], inplace=True)

    if metric == "거래량":
        df = df.dropna(subset=['Volume'])
        sorted_df = df.sort_values(by="Volume", ascending=False)
    elif metric == "상승률":
        # 전일 종가 대비 등락률 계산
        df = df.dropna(subset=['Close', 'Previous_Close'])
        df = df[df['Previous_Close'] > 0]  # 0으로 나누는 것 방지
        df['Change'] = (df['Close'] - df['Previous_Close']) / df['Previous_Close'] * 100
        sorted_df = df.sort_values(by="Change", ascending=False)
    elif metric == "하락률":
        # 전일 종가 대비 등락률 계산 (하락률이 큰 순서)
        df = df.dropna(subset=['Close', 'Previous_Close'])
        df = df[df['Previous_Close'] > 0]  # 0으로 나누는 것 방지
        df['Change'] = (df['Close'] - df['Previous_Close']) / df['Previous_Close'] * 100
        sorted_df = df.sort_values(by="Change", ascending=True)
    elif metric == "가격":
        df = df.dropna(subset=['Close'])
        sorted_df = df.sort_values(by="Close", ascending=False)
    else:
        return f"지원하지 않는 지표: {metric}"

    top_stocks = sorted_df.head(n)

    if top_stocks.empty:
        return f"{date}의 {market} 시장에서 해당 지표({metric})로 순위를 매길 수 있는 종목이 없습니다."

    # 종목 이름만 추출하여 쉼표로 구분된 문자열로 반환
    return ", ".join(top_stocks['stock_name'].tolist())

# 4. 시장 통계 조회 (상승/하락 종목 수, 거래대금 등)
def get_market_statistics(**kwargs):
    '''
    - 특정 날짜의 시장 통계를 조회 (상승/하락 종목 수, 전체 거래대금 등)
    '''
    date = kwargs.get('date')
    stat_type = kwargs.get('stat_type')  # 'rising_count', 'falling_count', 'total_trading_value', 'kospi_rising', 'kosdaq_rising'
    market = kwargs.get('market', None)  # 특정 시장 지정 시 사용
    
    try:
        date_formatted = date.replace('-', '')
        
        if stat_type == 'rising_count':
            # 전체 시장 상승 종목 수
            kospi_rising = len([t for t in stock.get_market_ticker_list(date_formatted, "KOSPI") 
                              if _is_stock_rising(t, date)])
            kosdaq_rising = len([t for t in stock.get_market_ticker_list(date_formatted, "KOSDAQ") 
                               if _is_stock_rising(t, date)])
            total_rising = kospi_rising + kosdaq_rising
            return f"{total_rising}개"
            
        elif stat_type == 'falling_count':
            # 전체 시장 하락 종목 수
            kospi_falling = len([t for t in stock.get_market_ticker_list(date_formatted, "KOSPI") 
                               if _is_stock_falling(t, date)])
            kosdaq_falling = len([t for t in stock.get_market_ticker_list(date_formatted, "KOSDAQ") 
                                if _is_stock_falling(t, date)])
            total_falling = kospi_falling + kosdaq_falling
            return f"{total_falling}개"
            
        elif stat_type == 'total_trading_value':
            # 전체 시장 거래대금
            kospi_value = stock.get_market_trading_value_by_date(date_formatted, date_formatted, "KOSPI")
            kosdaq_value = stock.get_market_trading_value_by_date(date_formatted, date_formatted, "KOSDAQ")
            if not kospi_value.empty and not kosdaq_value.empty:
                total_value = kospi_value.iloc[0]['거래대금'] + kosdaq_value.iloc[0]['거래대금']
                return f"{total_value:,}원"
            return "거래대금 데이터를 가져올 수 없습니다."
            
        elif stat_type == 'market_rising_count' and market:
            # 특정 시장의 상승 종목 수
            rising_count = len([t for t in stock.get_market_ticker_list(date_formatted, market) 
                              if _is_stock_rising(t, date)])
            return f"{rising_count}개"
            
        elif stat_type == 'market_traded_count' and market:
            # 특정 시장의 거래된 종목 수
            tickers = stock.get_market_ticker_list(date_formatted, market)
            traded_count = len([t for t in tickers if _has_trading_data(t, date)])
            return f"{traded_count}개"
            
        return "지원하지 않는 통계 유형입니다."
        
    except Exception as e:
        return f"시장 통계 조회 중 오류 발생: {e}"

def _is_stock_rising(ticker, date):
    '''티커의 해당 날짜 상승 여부 확인 (전일 종가 대비)'''
    try:
        date_formatted = date.replace('-', '')
        # 해당 날짜와 이전 거래일 데이터 조회
        df = stock.get_market_ohlcv_by_ticker(date_formatted, ticker)
        if not df.empty:
            current_close = df.iloc[0]['종가']
            
            # 이전 거래일 조회 (최대 5일 전까지)
            for days_back in range(1, 6):
                prev_date_obj = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=days_back)
                prev_date_formatted = prev_date_obj.strftime("%Y%m%d")
                try:
                    prev_df = stock.get_market_ohlcv_by_ticker(prev_date_formatted, ticker)
                    if not prev_df.empty:
                        prev_close = prev_df.iloc[0]['종가']
                        return current_close > prev_close
                except:
                    continue
    except:
        pass
    return False

def _is_stock_falling(ticker, date):
    '''티커의 해당 날짜 하락 여부 확인 (전일 종가 대비)'''
    try:
        date_formatted = date.replace('-', '')
        # 해당 날짜와 이전 거래일 데이터 조회
        df = stock.get_market_ohlcv_by_ticker(date_formatted, ticker)
        if not df.empty:
            current_close = df.iloc[0]['종가']
            
            # 이전 거래일 조회 (최대 5일 전까지)
            for days_back in range(1, 6):
                prev_date_obj = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=days_back)
                prev_date_formatted = prev_date_obj.strftime("%Y%m%d")
                try:
                    prev_df = stock.get_market_ohlcv_by_ticker(prev_date_formatted, ticker)
                    if not prev_df.empty:
                        prev_close = prev_df.iloc[0]['종가']
                        return current_close < prev_close
                except:
                    continue
    except:
        pass
    return False

def _has_trading_data(ticker, date):
    '''티커의 해당 날짜 거래 데이터 존재 여부 확인'''
    try:
        df = stock.get_market_ohlcv_by_ticker(date.replace('-', ''), ticker)
        return not df.empty and df.iloc[0]['거래량'] > 0
    except:
        pass
    return False

# 5. 전체 시장 거래량 순위 조회
def get_all_market_volume_ranking(**kwargs):
    '''
    - 전체 시장(KOSPI + KOSDAQ)에서 거래량 기준 상위 N개 종목 조회
    '''
    date = kwargs.get('date')
    n = int(kwargs.get('n', 10))
    
    try:
        # KOSPI와 KOSDAQ 각각에서 상위 종목들을 가져와서 합치기
        kospi_top = get_top_stocks_by_metric(date=date, market="KOSPI", metric="거래량", n=n*2)
        kosdaq_top = get_top_stocks_by_metric(date=date, market="KOSDAQ", metric="거래량", n=n*2)
        
        # 실제 거래량 데이터를 포함하여 재정렬
        all_stocks = []
        
        # KOSPI 종목들의 거래량 데이터 수집
        if kospi_top and "," in kospi_top:
            for stock_name in kospi_top.split(", "):
                ticker = get_ticker(stock_name)
                if ticker:
                    hist = get_history(ticker, date)
                    if hist is not None:
                        all_stocks.append({
                            'name': stock_name, 
                            'volume': int(hist['Volume']),
                            'market': 'KOSPI'
                        })
        
        # KOSDAQ 종목들의 거래량 데이터 수집  
        if kosdaq_top and "," in kosdaq_top:
            for stock_name in kosdaq_top.split(", "):
                ticker = get_ticker(stock_name)
                if ticker:
                    hist = get_history(ticker, date)
                    if hist is not None:
                        all_stocks.append({
                            'name': stock_name, 
                            'volume': int(hist['Volume']),
                            'market': 'KOSDAQ'
                        })
        
        # 거래량 기준 정렬하여 상위 N개 반환
        all_stocks.sort(key=lambda x: x['volume'], reverse=True)
        top_n_stocks = all_stocks[:n]
        
        return ", ".join([stock['name'] for stock in top_n_stocks])
        
    except Exception as e:
        return f"전체 시장 거래량 순위 조회 중 오류 발생: {e}"

# 6. 특정 시장의 거래량 1위 종목 (거래량 수치 포함)
def get_top_volume_stock_with_count(**kwargs):
    '''
    - 특정 시장에서 거래량 1위 종목과 거래량 수치를 함께 반환
    '''
    date = kwargs.get('date')
    market = kwargs.get('market')
    
    try:
        top_stock = get_top_stocks_by_metric(date=date, market=market, metric="거래량", n=1)
        if top_stock and top_stock != f"{date}의 {market} 시장에서 해당 지표(거래량)로 순위를 매길 수 있는 종목이 없습니다.":
            # 거래량 수치 조회
            ticker = get_ticker(top_stock)
            if ticker:
                hist = get_history(ticker, date)
                if hist is not None:
                    volume = int(hist['Volume'])
                    return f"{top_stock} ({volume:,}주)"
        
        return f"{date} {market} 시장의 거래량 1위 종목 정보를 가져올 수 없습니다."
        
    except Exception as e:
        return f"거래량 1위 종목 조회 중 오류 발생: {e}"

# 7. 모호한 질문 처리 및 되묻기
def ask_for_clarification(**kwargs):
    '''
    - 모호한 질문에 대해 구체적인 정보를 요청
    '''
    question_type = kwargs.get('question_type')
    missing_info = kwargs.get('missing_info', [])
    
    clarification_messages = {
        'recent_rising_stocks': '최근 상승한 주식을 조회하려면 다음 정보가 필요합니다:',
        'stocks_down_from_high': '고점 대비 하락한 주식을 조회하려면 다음 정보가 필요합니다:',
        'general_inquiry': '더 정확한 답변을 위해 다음 정보를 명시해 주세요:'
    }
    
    base_message = clarification_messages.get(question_type, '더 구체적인 정보가 필요합니다:')
    
    missing_details = []
    for info in missing_info:
        if info == 'date':
            missing_details.append('- 조회하고 싶은 날짜 (예: 2024-12-01)')
        elif info == 'market':
            missing_details.append('- 시장 구분 (KOSPI 또는 KOSDAQ)')
        elif info == 'period':
            missing_details.append('- 기간 설정 (예: 최근 1주일, 1개월 등)')
        elif info == 'count':
            missing_details.append('- 조회할 종목 개수 (예: 상위 5개, 10개 등)')
        elif info == 'criteria':
            missing_details.append('- 기준 설정 (상승률, 거래량, 가격 등)')
    
    if missing_details:
        return f"{base_message}\n" + "\n".join(missing_details)
    else:
        return "질문을 더 구체적으로 설명해 주시면 정확한 답변을 드릴 수 있습니다."

# 8. 최근 상승 주식 조회 (기본값 포함)
def get_recent_rising_stocks(**kwargs):
    '''
    - "최근 많이 오른 주식" 같은 모호한 질문에 대한 기본값 처리
    '''
    date = kwargs.get('date', _get_previous_trading_day())  # 기본값: 최근 거래일
    market = kwargs.get('market', 'ALL')  # 기본값: 전체 시장
    n = int(kwargs.get('n', 5))  # 기본값: 상위 5개
    period_days = int(kwargs.get('period_days', 1))  # 기본값: 1일
    
    try:
        if market == 'ALL':
            # KOSPI와 KOSDAQ 모두에서 상승률 상위 종목 조회
            kospi_rising = get_top_stocks_by_metric(date=date, market="KOSPI", metric="상승률", n=n)
            kosdaq_rising = get_top_stocks_by_metric(date=date, market="KOSDAQ", metric="상승률", n=n)
            
            return f"📈 {date} 상승률 상위 종목:\n[KOSPI] {kospi_rising}\n[KOSDAQ] {kosdaq_rising}"
        else:
            rising_stocks = get_top_stocks_by_metric(date=date, market=market, metric="상승률", n=n)
            return f"📈 {date} {market} 상승률 상위 {n}개 종목: {rising_stocks}"
            
    except Exception as e:
        return f"최근 상승 주식 조회 중 오류 발생: {e}"

# 9. 고점 대비 하락한 주식 조회 (개선된 버전)
def get_stocks_down_from_high(**kwargs):
    '''
    - 52주 고점 대비 하락률이 큰 주식들을 조회
    '''
    date = kwargs.get('date', _get_previous_trading_day())  # 기본값: 최근 거래일
    market = kwargs.get('market', 'ALL')  # 기본값: 전체 시장
    n = int(kwargs.get('n', 5))  # 기본값: 상위 5개
    weeks = int(kwargs.get('weeks', 52))  # 기본값: 52주
    
    try:
        # 52주 전 날짜 계산
        current_date = datetime.strptime(date, "%Y-%m-%d")
        start_date = current_date - timedelta(weeks=weeks)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        tickers = _get_all_market_tickers(market) if market != 'ALL' else _get_all_market_tickers()
        if not tickers:
            return f"시장 정보를 가져올 수 없습니다."
        
        # 청크 단위로 처리하여 메모리 최적화 및 오류 억제
        stocks_with_decline = []
        chunk_size = 30  # 52주 데이터를 가져오므로 청크 크기를 더 줄임
        
        for i in range(0, min(len(tickers), 150), chunk_size):  # 최대 150개 종목만 처리
            chunk_tickers = tickers[i:i+chunk_size]
            
            # 안전한 다운로드 사용
            data = _safe_yf_download(chunk_tickers, start_date_str, date)
            
            if data.empty:
                continue
                
            for ticker in chunk_tickers:
                try:
                    if ticker in data.columns.get_level_values(0):
                        ticker_data = data[ticker]
                        if not ticker_data.empty:
                            # 52주 고점과 현재가 계산
                            high_52w = ticker_data['High'].max()
                            current_price = ticker_data['Close'].iloc[-1]
                            
                            if pd.notna(high_52w) and pd.notna(current_price) and high_52w > 0:
                                decline_pct = ((current_price - high_52w) / high_52w) * 100
                                
                                # 종목명 조회
                                stock_name = None
                                if _KRX_TICKER_CACHE:
                                    for name, t in _KRX_TICKER_CACHE.items():
                                        if t == ticker:
                                            stock_name = name
                                            break
                                
                                if stock_name and decline_pct < -5:  # 5% 이상 하락한 종목만
                                    stocks_with_decline.append({
                                        'name': stock_name,
                                        'decline_pct': decline_pct,
                                        'current_price': current_price,
                                        'high_52w': high_52w
                                    })
                except Exception:
                    continue
        
        if not stocks_with_decline:
            return f"고점 대비 하락한 종목을 찾을 수 없습니다."
        
        # 하락률 기준으로 정렬 (하락률이 큰 순서)
        stocks_with_decline.sort(key=lambda x: x['decline_pct'])
        top_declining = stocks_with_decline[:n]
        
        result_names = [stock['name'] for stock in top_declining]
        return ", ".join(result_names)
        
    except Exception as e:
        return f"고점 대비 하락 주식 조회 중 오류 발생: {e}"

# 10. 두 종목 비교
def compare_stocks(**kwargs):
    '''
    - 두 종목의 특정 지표를 비교하여 더 높은/낮은 종목을 반환
    '''
    date = kwargs.get('date')
    stock1 = kwargs.get('stock1')
    stock2 = kwargs.get('stock2')
    metric = kwargs.get('metric')  # '종가', '등락률', '거래량' 등
    comparison = kwargs.get('comparison', 'higher')  # 'higher' 또는 'lower'
    
    try:
        # 두 종목의 데이터 조회
        result1 = get_stock_metric(date=date, stock_name=stock1, metric=metric)
        result2 = get_stock_metric(date=date, stock_name=stock2, metric=metric)
        
        # 오류 체크
        if "정보를 찾을 수 없습니다" in result1 or "데이터가 없습니다" in result1:
            return f"{stock1}의 {date} {metric} 데이터를 가져올 수 없습니다."
        if "정보를 찾을 수 없습니다" in result2 or "데이터가 없습니다" in result2:
            return f"{stock2}의 {date} {metric} 데이터를 가져올 수 없습니다."
        
        # 숫자 값 추출
        def extract_number(text):
            import re
            if '%' in text:
                return float(re.search(r'[+-]?\d+\.?\d*', text).group())
            elif '원' in text:
                return float(re.search(r'\d+(?:,\d+)*', text).group().replace(',', ''))
            elif '주' in text:
                return float(re.search(r'\d+(?:,\d+)*', text).group().replace(',', ''))
            return 0
        
        value1 = extract_number(result1)
        value2 = extract_number(result2)
        
        if comparison == 'higher':
            winner = stock1 if value1 > value2 else stock2
            winner_value = result1 if value1 > value2 else result2
        else:  # 'lower'
            winner = stock1 if value1 < value2 else stock2
            winner_value = result1 if value1 < value2 else result2
        
        return f"{winner} ({winner_value})"
        
    except Exception as e:
        return f"종목 비교 중 오류 발생: {e}"

# 11. 시장 지수 비교
def compare_market_indices(**kwargs):
    '''
    - KOSPI와 KOSDAQ 지수를 비교
    '''
    date = kwargs.get('date')
    comparison = kwargs.get('comparison', 'higher')  # 'higher' 또는 'lower'
    
    try:
        kospi_result = get_market_index(date=date, market="KOSPI")
        kosdaq_result = get_market_index(date=date, market="KOSDAQ")
        
        # 오류 체크
        if "데이터가 없습니다" in kospi_result:
            return f"KOSPI의 {date} 지수 데이터를 가져올 수 없습니다."
        if "데이터가 없습니다" in kosdaq_result:
            return f"KOSDAQ의 {date} 지수 데이터를 가져올 수 없습니다."
        
        kospi_value = float(kospi_result)
        kosdaq_value = float(kosdaq_result)
        
        if comparison == 'higher':
            winner = "KOSPI" if kospi_value > kosdaq_value else "KOSDAQ"
            winner_value = kospi_result if kospi_value > kosdaq_value else kosdaq_result
        else:  # 'lower'
            winner = "KOSPI" if kospi_value < kosdaq_value else "KOSDAQ"
            winner_value = kospi_result if kospi_value < kosdaq_value else kosdaq_result
        
        return f"{winner} ({winner_value})"
        
    except Exception as e:
        return f"시장 지수 비교 중 오류 발생: {e}"

# 12. 시장 평균 등락률 계산
def calculate_market_average_change(**kwargs):
    '''
    - 특정 시장의 평균 등락률을 계산
    '''
    date = kwargs.get('date')
    market = kwargs.get('market', 'KOSPI')
    
    try:
        # 해당 시장의 주요 종목들의 등락률을 계산
        tickers = _get_all_market_tickers(market)
        if not tickers:
            return f"{market} 시장 정보를 가져올 수 없습니다."
        
        # 샘플링: 너무 많은 종목을 다 계산하면 시간이 오래 걸리므로 상위 50개만
        sample_tickers = tickers[:50]
        changes = []
        
        for ticker in sample_tickers:
            try:
                # 각 종목의 등락률을 정확하게 계산 (전일 종가 대비)
                target_data, previous_data = get_history_with_previous(ticker, date)
                if (target_data is not None and previous_data is not None and 
                    not pd.isna(target_data['Close']) and not pd.isna(previous_data['Close']) and 
                    previous_data['Close'] > 0):
                    
                    change = (target_data['Close'] - previous_data['Close']) / previous_data['Close'] * 100
                    changes.append(change)
            except:
                continue
                
        if not changes:
            return f"{date} {market} 시장의 평균 등락률을 계산할 수 없습니다."
        
        avg_change = sum(changes) / len(changes)
        return f"{avg_change:+.2f}%"
        
    except Exception as e:
        return f"시장 평균 등락률 계산 중 오류 발생: {e}"

# 13. 종목과 시장 평균 비교
def compare_stock_to_market(**kwargs):
    '''
    - 특정 종목의 등락률을 시장 평균과 비교
    '''
    date = kwargs.get('date')
    stock_name = kwargs.get('stock_name')
    market = kwargs.get('market', 'KOSPI')
    
    try:
        # 종목 등락률 조회
        stock_change_str = get_stock_metric(date=date, stock_name=stock_name, metric='등락률')
        if "정보를 찾을 수 없습니다" in stock_change_str or "데이터가 없습니다" in stock_change_str:
            return f"{stock_name}의 {date} 등락률 데이터를 가져올 수 없습니다."
        
        # 시장 평균 등락률 계산
        market_avg_str = calculate_market_average_change(date=date, market=market)
        if "계산할 수 없습니다" in market_avg_str:
            return f"{date} {market} 시장 평균 등락률을 계산할 수 없습니다."
        
        # 숫자 값 추출
        import re
        stock_change = float(re.search(r'[+-]?\d+\.?\d*', stock_change_str).group())
        market_avg = float(re.search(r'[+-]?\d+\.?\d*', market_avg_str).group())
        
        result = "높습니다" if stock_change > market_avg else "낮습니다"
        return f"{result} ({stock_name}: {stock_change_str}, {market} 평균: {market_avg_str})"
        
    except Exception as e:
        return f"종목과 시장 평균 비교 중 오류 발생: {e}"

# 14. 종목의 시장 거래량 점유율 계산 (pykrx 기반으로 개선)
def calculate_stock_volume_share(**kwargs):
    '''
    - 특정 종목의 거래량이 전체 시장 거래량에서 차지하는 비율 계산
    '''
    date = kwargs.get('date')
    stock_name = kwargs.get('stock_name')
    
    try:
        # 날짜 형식 변환
        date_formatted = date.replace('-', '')
        
        # 해당 종목의 티커 찾기
        ticker = get_ticker(stock_name)
        if not ticker:
            return f"'{stock_name}'에 대한 티커 정보를 찾을 수 없습니다."
        
        # 티커에서 숫자 부분만 추출 (pykrx용)
        import re
        ticker_code = re.search(r'\d+', ticker).group()
        
        # pykrx로 해당 종목의 거래량 조회
        try:
            stock_data = stock.get_market_ohlcv_by_ticker(date_formatted, ticker_code)
            if stock_data.empty:
                return f"{date}에 '{stock_name}'의 거래 데이터가 없습니다."
            stock_volume = stock_data.iloc[0]['거래량']
        except:
            return f"{date}에 '{stock_name}'의 거래 데이터를 가져올 수 없습니다."
        
        # 전체 시장 거래량 계산 (KOSPI + KOSDAQ)
        total_volume = 0
        
        # KOSPI 전체 거래량
        try:
            kospi_tickers = stock.get_market_ticker_list(date_formatted, market="KOSPI")
            for ticker_code in kospi_tickers:
                try:
                    data = stock.get_market_ohlcv_by_ticker(date_formatted, ticker_code)
                    if not data.empty:
                        total_volume += data.iloc[0]['거래량']
                except:
                    continue
        except:
            pass
        
        # KOSDAQ 전체 거래량
        try:
            kosdaq_tickers = stock.get_market_ticker_list(date_formatted, market="KOSDAQ")
            for ticker_code in kosdaq_tickers:
                try:
                    data = stock.get_market_ohlcv_by_ticker(date_formatted, ticker_code)
                    if not data.empty:
                        total_volume += data.iloc[0]['거래량']
                except:
                    continue
        except:
            pass
        
        if total_volume == 0:
            return f"{date} 전체 시장 거래량을 계산할 수 없습니다."
        
        share_pct = (stock_volume / total_volume) * 100
        return f"{share_pct:.4f}%"
        
    except Exception as e:
        return f"시장 거래량 점유율 계산 중 오류 발생: {e}"

# 15. 특정 종목의 거래량 순위 조회 (pykrx 기반으로 개선)
def get_stock_volume_rank(**kwargs):
    '''
    - 특정 종목의 전체 시장에서의 거래량 순위를 조회
    '''
    date = kwargs.get('date')
    stock_name = kwargs.get('stock_name')
    market = kwargs.get('market', 'ALL')  # 기본값: 전체 시장
    
    try:
        # 날짜 형식 변환
        date_formatted = date.replace('-', '')
        
        # 해당 종목의 티커 찾기
        ticker = get_ticker(stock_name)
        if not ticker:
            return f"'{stock_name}'에 대한 티커 정보를 찾을 수 없습니다."
        
        # 티커에서 숫자 부분만 추출 (pykrx용)
        import re
        target_ticker_code = re.search(r'\d+', ticker).group()
        
        # 해당 종목의 거래량 조회
        try:
            target_data = stock.get_market_ohlcv_by_ticker(date_formatted, target_ticker_code)
            if target_data.empty:
                return f"{date}에 '{stock_name}'의 거래 데이터가 없습니다."
            target_volume = target_data.iloc[0]['거래량']
        except:
            return f"{date}에 '{stock_name}'의 거래 데이터를 가져올 수 없습니다."
        
        # 전체 시장 거래량 데이터 수집
        all_volumes = []
        
        # 조회할 시장 결정
        markets_to_check = []
        if market == 'ALL':
            markets_to_check = ['KOSPI', 'KOSDAQ']
        else:
            markets_to_check = [market]
        
        for market_name in markets_to_check:
            try:
                tickers = stock.get_market_ticker_list(date_formatted, market=market_name)
                for ticker_code in tickers:
                    try:
                        data = stock.get_market_ohlcv_by_ticker(date_formatted, ticker_code)
                        if not data.empty:
                            volume = data.iloc[0]['거래량']
                            all_volumes.append({
                                'ticker': ticker_code,
                                'volume': volume,
                                'market': market_name
                            })
                    except:
                        continue
            except Exception as e:
                print(f"Error fetching {market_name} data: {e}")
                continue
        
        if not all_volumes:
            return f"{date} 시장 거래량 데이터를 가져올 수 없습니다."
        
        # 거래량 기준으로 정렬 (높은 순서)
        all_volumes.sort(key=lambda x: x['volume'], reverse=True)
        
        # 해당 종목의 순위 찾기
        rank = None
        for i, data in enumerate(all_volumes):
            if data['ticker'] == target_ticker_code:
                rank = i + 1
                break
        
        if rank is None:
            # 직접 매칭이 안 되면 거래량으로 추정
            for i, data in enumerate(all_volumes):
                if data['volume'] <= target_volume:
                    rank = i + 1
                    break
        
        if rank is None:
            rank = len(all_volumes) + 1
        
        market_text = f"{market} 시장" if market != 'ALL' else "전체 시장"
        return f"{rank}위 (총 {len(all_volumes)}개 종목 중 {market_text})"
        
    except Exception as e:
        return f"거래량 순위 조회 중 오류 발생: {e}"

# 16. 시가총액 계산
def calculate_market_cap(**kwargs):
    '''
    - 특정 종목의 시가총액을 계산 (종가 × 상장주식수)
    '''
    date = kwargs.get('date')
    stock_name = kwargs.get('stock_name')
    
    try:
        ticker = get_ticker(stock_name)
        if not ticker:
            return f"'{stock_name}'에 대한 티커 정보를 찾을 수 없습니다."
        
        # 주가 데이터 조회
        hist = get_history(ticker, date)
        if hist is None:
            return f"{date}에 '{stock_name}'의 거래 데이터가 없습니다."
        
        close_price = hist['Close']
        
        # yfinance를 통해 주식 정보 조회 (상장주식수 포함)
        with SuppressOutput():
            stock_info = yf.Ticker(ticker)
            info = stock_info.info
        
        # 상장주식수 조회 (여러 필드 시도)
        shares_outstanding = None
        for field in ['sharesOutstanding', 'impliedSharesOutstanding', 'floatShares']:
            if field in info and info[field] is not None:
                shares_outstanding = info[field]
                break
        
        if shares_outstanding is None or shares_outstanding == 0:
            return f"{stock_name}의 상장주식수 정보를 가져올 수 없습니다."
        
        # 시가총액 계산 (원 단위)
        market_cap = close_price * shares_outstanding
        
        # 조 단위로 변환
        market_cap_trillion = market_cap / 1e12
        
        if market_cap_trillion >= 1:
            return f"{market_cap_trillion:.2f}조원"
        else:
            market_cap_billion = market_cap / 1e8
            return f"{market_cap_billion:.0f}억원"
        
    except Exception as e:
        return f"시가총액 계산 중 오류 발생: {e}"

# 17. 시가총액 비교
def compare_market_caps(**kwargs):
    '''
    - 두 종목의 시가총액을 비교
    '''
    date = kwargs.get('date')
    stock1 = kwargs.get('stock1')
    stock2 = kwargs.get('stock2')
    comparison = kwargs.get('comparison', 'higher')  # 'higher' 또는 'lower'
    
    try:
        # 두 종목의 시가총액 계산
        market_cap1_str = calculate_market_cap(date=date, stock_name=stock1)
        market_cap2_str = calculate_market_cap(date=date, stock_name=stock2)
        
        # 오류 체크
        if "정보를 찾을 수 없습니다" in market_cap1_str or "데이터가 없습니다" in market_cap1_str or "계산 중 오류" in market_cap1_str:
            return f"{stock1}의 {date} 시가총액 데이터를 가져올 수 없습니다."
        if "정보를 찾을 수 없습니다" in market_cap2_str or "데이터가 없습니다" in market_cap2_str or "계산 중 오류" in market_cap2_str:
            return f"{stock2}의 {date} 시가총액 데이터를 가져올 수 없습니다."
        
        # 숫자 값 추출 (조원 또는 억원 단위)
        def extract_market_cap_value(text):
            import re
            if '조원' in text:
                return float(re.search(r'\d+\.?\d*', text).group()) * 1e12
            elif '억원' in text:
                return float(re.search(r'\d+\.?\d*', text).group()) * 1e8
            return 0
        
        value1 = extract_market_cap_value(market_cap1_str)
        value2 = extract_market_cap_value(market_cap2_str)
        
        if comparison == 'higher':
            winner = stock1 if value1 > value2 else stock2
            winner_value = market_cap1_str if value1 > value2 else market_cap2_str
        else:  # 'lower'
            winner = stock1 if value1 < value2 else stock2
            winner_value = market_cap1_str if value1 < value2 else market_cap2_str
        
        return f"{winner} ({winner_value})"
        
    except Exception as e:
        return f"시가총액 비교 중 오류 발생: {e}"

# --- v0.2 New Functions ---
def extract_json_body(text: str):
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group()
    raise ValueError("JSON body not found")

def normalize_conditions(parsed):
    for key in ["volume_ratio", "volume_absolute", "price_change", "min_price", "max_price"]:
        if key in parsed and isinstance(parsed[key], (int, float)):
            parsed[key] = {"operator": ">=", "value": parsed[key]}
    return parsed

def parse_question_with_llm_clova(question: str):
    prompt = (
        "다음 문장에서 조건검색에 필요한 정보를 JSON 형식으로 추출하세요.\n"
        "가능한 키: date, market, volume_ratio, volume_absolute, price_change, min_price, max_price\n"
        "출력은 반드시 JSON 형식만 포함되어야 합니다.\n"
        "\n"
        """예시: 2025년 5월 2일에 종가가 10만원 이상 30만원 이하인 종목 보여줘  
        → {"date": "2025-05-02", "min_price": {"operator": ">=", "value": 100000}, "max_price": {"operator": "<=", "value": 300000}, "market": "ALL"}

        예시: 2024년 9월 15일에 거래량이 전일 대비 500% 이상 증가한 KOSDAQ 종목 알려줘  
        → {"date": "2024-09-15", "volume_ratio": {"operator": ">=", "value": 500}, "market": "KOSDAQ"}

        예시: 2025년 3월 10일에 20일 평균 거래량이 100만 이상인 종목 보여줘  
        → {"date": "2025-03-10", "volume_absolute": {"operator": ">=", "value": 1000000}, "market": "ALL"}

        예시: 2024년 11월 7일에 등락률이 -3% 이하로 떨어진 KOSPI 종목 알려줘  
        → {"date": "2024-11-07", "price_change": {"operator": "<=", "value": -3}, "market": "KOSPI"}

        예시: 2025년 6월 1일에 종가가 20만원 이상인 종목은?  
        → {"date": "2025-06-01", "min_price": {"operator": ">=", "value": 200000}, "market": "ALL"}

        예시: 2025년 7월 18일에 종가가 15만원 이하이면서 거래량이 전일 대비 200% 이상 증가한 종목 보여줘  
        → {"date": "2025-07-18", "max_price": {"operator": "<=", "value": 150000}, "volume_ratio": {"operator": ">=", "value": 200}, "market": "ALL"}"""

        f"질문: \"{question}\""
    )
    try:
        raw = llm.invoke(prompt).content
        return normalize_conditions(json.loads(extract_json_body(raw)))
    except:
        return {}
    
def parse_tech_signal_question(question: str):
    prompt = (
        "다음 문장에서 기술적 분석 조건을 JSON으로 추출하세요.\n"
        "- date: YYYY-MM-DD 또는 기간 지정시 start_date, end_date\n"
        "- market: KOSPI, KOSDAQ, ALL 중 택1\n"
        "- indicator: bollinger_band, ma20_breakout, rsi, cross, volume_ratio 중 택1\n"
        "- signal_type: touch_lower, touch_upper, below, above, above_ma, death_cross, golden_cross 중 택1\n"
        "- threshold: (선택) 수치 값 (예: 70 또는 10)\n"
        "- target: (선택) 개별 종목명을 명시한 경우\n"
        "예: 2025-03-10에 종가가 20일 이동평균보다 10% 이상 높은 종목을 알려줘\n"
        "답: {\"date\": \"2025-03-10\", \"indicator\": \"ma20_breakout\", \"signal_type\": \"above_ma\", \"threshold\": 10, \"market\": \"ALL\"}\n"
        "예: 씨유메디칼에서 2024-06-01부터 2025-06-30까지 데드크로스가 몇번 발생했어?\n"
        "답: {\"start_date\": \"2024-06-01\", \"end_date\": \"2025-06-30\", \"indicator\": \"cross\", \"signal_type\": \"death_cross\", \"target\": \"씨유메디칼\"}\n"
        f"질문: \"{question}\""
    )
    try:
        response = llm.invoke(prompt)
        return json.loads(extract_json_body(response.content))
    except Exception as e:
        print("⚠️ 파싱 실패:", e)
        return None
def parse_question_hybrid(q):
    answer = parse_question_with_llm_clova(q)
    print(answer)
    return answer

def dispatch(parsed):
    handlers = []
    if "volume_ratio" in parsed:
        handlers.append(handle_volume_ratio(parsed))
    if "volume_absolute" in parsed:
        handlers.append(handle_absolute_volume(parsed))
    if "price_change" in parsed:
        handlers.append(handle_price_change(parsed))
    if "min_price" in parsed and "max_price" in parsed:
        handlers.append(handle_price_range(parsed))
    if not handlers:
        return {"error": "❌ 조건을 만족하는 종목이 없습니다."}
    intersect = set(handlers[0])
    for h in handlers[1:]:
        intersect &= set(h)
    return list(intersect)
# ✅ 터치 판단 함수 (허용 오차 적용)
def check_bollinger_touch(row, signal_type, tolerance=0.005):
    if signal_type in ("touch_lower", "below"):
        return row["Close"] <= row["lower"] * (1 + tolerance)
    elif signal_type in ("touch_upper", "above"):
        return row["Close"] >= row["upper"] * (1 - tolerance)
    return False

# ✅ 볼린저 밴드 핸들러
def handle_bollinger(parsed):
    date = parsed["date"]
    signal_type = parsed["signal_type"]
    market = parsed.get("market", "ALL")

    krx = fdr.StockListing("KRX")
    if market == "KOSPI":
        krx = krx[krx["Market"] == "KOSPI"]
    elif market == "KOSDAQ":
        krx = krx[krx["Market"] == "KOSDAQ"]

    ticker_map = {}
    for _, row in krx.iterrows():
        code = row["Code"]
        suffix = ".KS" if row["Market"] == "KOSPI" else ".KQ"
        ticker_map[code + suffix] = row["Name"]

    def get_bollinger_result(ticker):
        try:
            end_dt = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
            df = yf.Ticker(ticker).history(start=(datetime.strptime(date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"))
            df.index = df.index.strftime("%Y-%m-%d")
            if date not in df.index or len(df) < 20:
                return None
            df["middle"] = df["Close"].rolling(20).mean()
            df["std"] = df["Close"].rolling(20).std()
            df["upper"] = df["middle"] + 2 * df["std"]
            df["lower"] = df["middle"] - 2 * df["std"]
            row = df.loc[date]
            if check_bollinger_touch(row, signal_type):
                return {
                    "name": ticker_map[ticker],
                    "close": round(row["Close"]),
                    "upper": round(row["upper"]),
                    "lower": round(row["lower"])
                }
            return None
        except:
            return None

    print(f"⏳ 볼린저 밴드 '{signal_type}' 조건 탐색 중...")
    results = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = executor.map(get_bollinger_result, list(ticker_map.keys()))
        for r in futures:
            if r:
                results.append(r)

    if not results:
        print("📭 조건을 만족하는 종목이 없습니다.")
        return []

    for r in results:
        print(f"📌 {r['name']} - 종가 {r['close']}원 / 상단:{r['upper']} / 하단:{r['lower']}")
    return [f"{r['name']}(종가:{r['close']} / 상단:{r['upper']} / 하단:{r['lower']})" for r in results]
# ✅ RSI 핸들러
def handle_rsi(parsed):
    date = parsed["date"]
    threshold = parsed["threshold"]
    direction = parsed["signal_type"]
    market = parsed.get("market", "ALL")

    krx = fdr.StockListing("KRX")
    if market == "KOSPI":
        krx = krx[krx["Market"] == "KOSPI"]
    elif market == "KOSDAQ":
        krx = krx[krx["Market"] == "KOSDAQ"]

    ticker_map = {}
    for _, row in krx.iterrows():
        code = row["Code"]
        suffix = ".KS" if row["Market"] == "KOSPI" else ".KQ"
        ticker_map[code + suffix] = row["Name"]

    def check_rsi(ticker):
        try:
            end_dt = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
            df = yf.Ticker(ticker).history(start=(datetime.strptime(date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"))
            df.index = df.index.strftime("%Y-%m-%d")
            if date not in df.index or len(df) < 15:
                return None
            delta = df["Close"].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.rolling(14).mean()
            avg_loss = loss.rolling(14).mean()
            rs = avg_gain / avg_loss
            df["rsi"] = 100 - (100 / (1 + rs))
            rsi_today = df.loc[date]["rsi"]
            if pd.isna(rsi_today):
                return None
            if direction == "above" and rsi_today >= threshold:
                return {"name": ticker_map[ticker], "rsi": round(rsi_today, 1)}
            elif direction == "below" and rsi_today <= threshold:
                return {"name": ticker_map[ticker], "rsi": round(rsi_today, 1)}
            return None
        except:
            return None

    print(f"⏳ RSI {direction} {threshold} 조건 탐색 중...")
    results = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = executor.map(check_rsi, list(ticker_map.keys()))
        for r in futures:
            if r:
                results.append(r)

    if not results:
        print("📭 조건을 만족하는 종목이 없습니다.")
        return []

    for r in results:
        print(f"📌 {r['name']} - RSI:{r['rsi']}")
    return [f"{r['name']}(RSI:{r['rsi']})" for r in results]

# ✅ 교차 핸들러 (기준선: MA5 vs MA20) - 멀티 signal_type 지원

def handle_cross(parsed):
    start_date = parsed["start_date"]
    end_date = parsed["end_date"]
    signal_types = parsed["signal_type"]
    if isinstance(signal_types, str):
        signal_types = [signal_types]
    target = parsed.get("target")
    market = parsed.get("market", "ALL")

    krx = fdr.StockListing("KRX")
    if market == "KOSPI":
        krx = krx[krx["Market"] == "KOSPI"]
    elif market == "KOSDAQ":
        krx = krx[krx["Market"] == "KOSDAQ"]

    results = []

    if target:
        krx = krx[krx["Name"] == target]

    def check_cross(code, name, suffix):
        ticker = code + suffix
        df = yf.Ticker(ticker).history(start=start_date, end=(datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"))
        if df.empty:
            return None

        df["ma_short"] = df["Close"].rolling(5).mean()
        df["ma_long"] = df["Close"].rolling(20).mean()
        df.dropna(inplace=True)

        counts = {"death_cross": 0, "golden_cross": 0}
        for i in range(1, len(df)):
            prev, curr = df.iloc[i - 1], df.iloc[i]
            cross_date = curr.name.tz_localize(None) if curr.name.tzinfo else curr.name
            if not (pd.to_datetime(start_date) <= cross_date <= pd.to_datetime(end_date)):
                continue
            if "death_cross" in signal_types and prev["ma_short"] > prev["ma_long"] and curr["ma_short"] <= curr["ma_long"]:
                counts["death_cross"] += 1
            if "golden_cross" in signal_types and prev["ma_short"] < prev["ma_long"] and curr["ma_short"] >= curr["ma_long"]:
                counts["golden_cross"] += 1

        total = sum(counts.values())
        if total > 0:
            return f"{name} - {', '.join([f'{k}:{v}회' for k, v in counts.items() if v > 0])}"
        return None

    print(f"🔍 교차 조건 탐색 중... ({', '.join(signal_types)})")
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for _, row in krx.iterrows():
            code = row["Code"]
            name = row["Name"]
            suffix = ".KS" if row["Market"] == "KOSPI" else ".KQ"
            futures.append(executor.submit(check_cross, code, name, suffix))

        for f in futures:
            result = f.result()
            if result:
                print("📌", result)
                results.append(result)

    if not results:
        print("📭 조건을 만족하는 종목이 없습니다.")
    return results




# ✅ MA20 돌파 핸들러
def handle_ma_breakout(parsed):
    date = parsed["date"]
    threshold = parsed["threshold"]
    market = parsed.get("market", "ALL")

    krx = fdr.StockListing("KRX")
    if market == "KOSPI":
        krx = krx[krx["Market"] == "KOSPI"]
    elif market == "KOSDAQ":
        krx = krx[krx["Market"] == "KOSDAQ"]

    ticker_map = {}
    for _, row in krx.iterrows():
        code = row["Code"]
        suffix = ".KS" if row["Market"] == "KOSPI" else ".KQ"
        ticker_map[code + suffix] = row["Name"]

    def check_ma_breakout(ticker):
        try:
            end_dt = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
            df = yf.Ticker(ticker).history(start=(datetime.strptime(date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"))
            df.index = df.index.strftime("%Y-%m-%d")
            if date not in df.index or len(df) < 20:
                return None
            df["ma20"] = df["Close"].rolling(20).mean()
            row = df.loc[date]
            ma_val = row["ma20"]
            if pd.isna(ma_val):
                return None
            if row["Close"] >= ma_val * (1 + threshold / 100):
                return {
                    "name": ticker_map[ticker],
                    "close": round(row["Close"]),
                    "ma20": round(ma_val),
                    "gap": round((row["Close"] - ma_val) / ma_val * 100, 2)
                }
            return None
        except:
            return None

    print(f"⏳ MA20 대비 {threshold}% 이상 상승 종목 탐색 중...")
    results = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = executor.map(check_ma_breakout, list(ticker_map.keys()))
        for r in futures:
            if r:
                results.append(r)

    if not results:
        print("📭 조건을 만족하는 종목이 없습니다.")
        return []

    for r in results:
        print(f"📌 {r['name']} - 종가:{r['close']} / MA20:{r['ma20']} / 괴리율:{r['gap']}%")
    return [f"{r['name']}(종가:{r['close']} / MA20:{r['ma20']} / +{r['gap']}%)" for r in results]
# ✅ 거래량 급등 핸들러
# ✅ 거래량 급등 핸들러 수정본

def handle_volume_ratio(parsed):
    date = parsed["date"]
    threshold = parsed["threshold"]
    n = parsed.get("volume_avg_n_days", 20)
    market = parsed.get("market", "ALL")

    krx = fdr.StockListing("KRX")
    if market == "KOSPI":
        krx = krx[krx["Market"] == "KOSPI"]
    elif market == "KOSDAQ":
        krx = krx[krx["Market"] == "KOSDAQ"]

    ticker_map = {}
    for _, row in krx.iterrows():
        code = row["Code"]
        suffix = ".KS" if row["Market"] == "KOSPI" else ".KQ"
        ticker_map[code + suffix] = row["Name"]

    def check_volume_ratio(ticker):
        try:
            end_dt = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
            start_dt = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=n * 2)
            df = yf.Ticker(ticker).history(start=start_dt.strftime("%Y-%m-%d"), end=end_dt.strftime("%Y-%m-%d"))
            df.index = df.index.strftime("%Y-%m-%d")
            if date not in df.index or len(df) < n:
                return None
            avg_volume = df.loc[:date].iloc[:-1]["Volume"].tail(n).mean()
            today_volume = df.loc[date]["Volume"]
            if (
                today_volume is None or avg_volume is None or
                pd.isna(today_volume) or pd.isna(avg_volume) or
                today_volume == 0 or avg_volume == 0
            ):
                return None
            if today_volume >= avg_volume * (threshold / 100):
                return {
                    "name": ticker_map[ticker],
                    "volume": int(today_volume),
                    "avg": int(avg_volume),
                    "ratio": round(today_volume / avg_volume * 100, 1)
                }
            return None
        except:
            return None

    print(f"⏳ 거래량 {n}일 평균 대비 {threshold}% 이상 종목 탐색 중...")
    results = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = executor.map(check_volume_ratio, list(ticker_map.keys()))
        for r in futures:
            if r:
                results.append(r)

    if not results:
        print("📭 조건을 만족하는 종목이 없습니다.")
        return []

    for r in results:
        print(f"📌 {r['name']} - 거래량:{r['volume']} / 평균:{r['avg']} / 비율:{r['ratio']}%")
    return [f"{r['name']}(거래량:{r['volume']} / 평균:{r['avg']} / {r['ratio']}%)" for r in results]

def dispatch_technical(parsed):
    indicator = parsed.get("indicator")
    if indicator == "bollinger_band":
        return handle_bollinger(parsed)
    elif indicator == "ma20_breakout":
        return handle_ma_breakout(parsed)
    elif indicator == "rsi":
        return handle_rsi(parsed)
    elif indicator == "cross":
        return handle_cross(parsed)
    elif indicator == "volume_ratio":
        return handle_volume_ratio(parsed)
    else:
        return ["❌ 지원하지 않는 기술적 분석 조건입니다."]
    
def handle_volume_ratio(p): return query_core(p, "ratio", p["volume_ratio"].get("value"))
def handle_absolute_volume(p): return query_core(p, "absolute", p["volume_absolute"].get("value"))
def handle_price_change(p): return query_core(p, "price_change", p["price_change"].get("value"))
def handle_price_range(p): return query_core(p, "price_range", (p["min_price"].get("value"), p["max_price"].get("value")))

def query_core(parsed, mode: str, threshold):
    date = parsed["date"]
    market = parsed.get("market", "ALL")
    krx = fdr.StockListing("KRX")
    if market == "KOSPI":
        krx = krx[krx["Market"] == "KOSPI"]
    elif market == "KOSDAQ":
        krx = krx[krx["Market"] == "KOSDAQ"]

    ticker_map = {}
    for _, row in krx.iterrows():
        code = row["Code"]
        suffix = ".KS" if row["Market"] == "KOSPI" else ".KQ"
        ticker = code + suffix
        ticker_map[ticker] = row["Name"]

    tickers = list(ticker_map.keys())
    target_date = datetime.strptime(date, "%Y-%m-%d")
    date_str = target_date.strftime("%Y-%m-%d")
    prev_str = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")

    def get_data(ticker):
        try:
            df = yf.Ticker(ticker).history(start=prev_str, end=(target_date + timedelta(days=1)).strftime("%Y-%m-%d"))
            df.index = df.index.strftime("%Y-%m-%d")
            if mode == "ratio" and prev_str in df.index and date_str in df.index:
                vol_y = df.loc[prev_str]["Volume"]
                vol_t = df.loc[date_str]["Volume"]
                if vol_y > 0 and ((vol_t - vol_y) / vol_y) * 100 >= threshold:
                    return ticker_map[ticker]
            elif mode == "absolute" and date_str in df.index:
                vol = df.loc[date_str]["Volume"]
                if vol >= threshold:
                    return ticker_map[ticker]
            elif mode == "price_change" and prev_str in df.index and date_str in df.index:
                close_y = df.loc[prev_str]["Close"]
                close_t = df.loc[date_str]["Close"]
                change = ((close_t - close_y) / close_y) * 100
                op = parsed["price_change"]["operator"]
                if close_y > 0:
                    if op == ">=" and change >= threshold:
                        return ticker_map[ticker]
                    elif op == "<=" and change <= threshold:
                        return ticker_map[ticker]
                    elif op == ">" and change > threshold:
                        return ticker_map[ticker]
                    elif op == "<" and change < threshold:
                        return ticker_map[ticker]
                    elif op == "==" and change == threshold:
                        return ticker_map[ticker]
            elif mode == "price_range" and date_str in df.index:
                close_t = df.loc[date_str]["Close"]
                min_p, max_p = threshold
                if min_p <= close_t <= max_p:
                    return ticker_map[ticker]
        except:
            return None

    print(f"⏳ {mode} 조건 계산 중...")
    with ThreadPoolExecutor(max_workers=32) as executor:
        results = list(executor.map(get_data, tickers))

    print(results)
    return [r for r in results if r]

def query_by_condition(**kwargs):
    question = kwargs.get('question')
    parsed = parse_question_hybrid(question)
    result = dispatch(parsed)
    return result if result else "📭 조건을 만족하는 종목이 없습니다."

def query_by_technical_signal(**kwargs):
    question = kwargs.get('question')
    parsed = parse_tech_signal_question(question)
    result = dispatch_technical(parsed)
    return result if result else "📭 조건을 만족하는 종목이 없습니다."

# --- 사용 가능한 모든 스킬(Tool)들을 이름으로 찾아쓸 수 있도록 딕셔너리로 관리 ---
SKILL_HANDLERS = {
    "get_stock_metric": get_stock_metric,
    "get_market_index": get_market_index,
    "get_top_stocks_by_metric": get_top_stocks_by_metric,
    "get_market_statistics": get_market_statistics,
    "get_all_market_volume_ranking": get_all_market_volume_ranking,
    "get_top_volume_stock_with_count": get_top_volume_stock_with_count,
    "ask_for_clarification": ask_for_clarification,
    "get_recent_rising_stocks": get_recent_rising_stocks,
    "get_stocks_down_from_high": get_stocks_down_from_high,
    "compare_stocks": compare_stocks,
    "compare_market_indices": compare_market_indices,
    "calculate_market_average_change": calculate_market_average_change,
    "compare_stock_to_market": compare_stock_to_market,
    "calculate_stock_volume_share": calculate_stock_volume_share,
    "get_stock_volume_rank": get_stock_volume_rank,
    "calculate_market_cap": calculate_market_cap,
    "compare_market_caps": compare_market_caps,
    "query_by_condition": query_by_condition,
    "query_by_technical_signal" : query_by_technical_signal
}
