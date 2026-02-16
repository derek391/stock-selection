import qstock as qs
import pandas as pd
import numpy as np
import datetime
import time
import warnings
import sys

warnings.filterwarnings('ignore')
pd.set_option('display.unicode.east_asian_width', True)

# ==========================================
# Cache Management Class
# ==========================================
class DataCache:
    """缓存管理，避免重复请求API"""
    
    def __init__(self, cache_duration=300):
        self.cache = {}
        self.cache_duration = cache_duration
        self.last_fetch_time = None
        self.realtime_data = None
        
    def get_realtime_data(self, force_refresh=False):
        current_time = time.time()
        
        if not force_refresh and self.realtime_data is not None and self.last_fetch_time:
            if current_time - self.last_fetch_time < self.cache_duration:
                print("📦 使用缓存的实时数据")
                return self.realtime_data
        
        try:
            print("🔄 正在获取实时行情数据...")
            df = qs.realtime_data()
            if df is not None and len(df) > 100:
                self.realtime_data = df
                self.last_fetch_time = current_time
                print(f"✅ 获取成功，共 {len(df)} 只股票")
                return df
        except Exception as e:
            print(f"❌ 获取失败: {e}")
            return None
    
    def get_history_data(self, code, start_date, end_date, force_refresh=False):
        cache_key = f"{code}_{start_date}_{end_date}"
        current_time = time.time()
        
        if not force_refresh and cache_key in self.cache:
            cache_time, data = self.cache[cache_key]
            if current_time - cache_time < self.cache_duration * 2:
                return data
        
        try:
            df = qs.get_data(code, start=start_date, end=end_date)
            if df is not None and not df.empty:
                self.cache[cache_key] = (current_time, df)
                return df
        except:
            return None
    
    def batch_get_history_data(self, code_list, start_date, end_date, show_progress=True):
        results = {}
        total = len(code_list)
        
        for i, code in enumerate(code_list):
            if show_progress and (i + 1) % 10 == 0:
                print(f"  进度: {i + 1}/{total}")
            
            cache_key = f"{code}_{start_date}_{end_date}"
            current_time = time.time()
            
            if cache_key in self.cache:
                cache_time, data = self.cache[cache_key]
                if current_time - cache_time < self.cache_duration * 2:
                    results[code] = data
                    continue
            
            try:
                time.sleep(0.02)
                df = qs.get_data(code, start=start_date, end=end_date)
                if df is not None and not df.empty:
                    self.cache[cache_key] = (current_time, df)
                    results[code] = df
            except:
                continue
        
        return results
    
    def clear_expired_cache(self):
        current_time = time.time()
        expired_keys = []
        
        for key, (cache_time, _) in self.cache.items():
            if current_time - cache_time > self.cache_duration * 3:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.cache[key]
        
        if expired_keys:
            print(f"🧹 已清除 {len(expired_keys)} 个过期缓存")

# 创建全局缓存实例
cache = DataCache()

# ==========================================
# 1. Helper Functions
# ==========================================
def clean_data(df):
    rename_map = {
        '代码': 'code', '名称': 'name', 
        '最新': 'close', '最新价': 'close', 
        '涨幅': 'pct_chg', '涨跌幅': 'pct_chg',
        '换手率': 'turnover', '换手': 'turnover',
        '市盈率': 'pe', '市盈率(动)': 'pe', 
        '成交量': 'volume', '成交额': 'amount',
        '量比': 'vol_ratio', '流通市值': 'float_mv'
    }
    existing_cols = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_cols)
    numeric_cols = ['close', 'pe', 'turnover', 'pct_chg', 'volume', 'amount', 'float_mv']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def is_gem_stock(code):
    try: 
        return str(code).zfill(6).startswith('300')
    except: 
        return False

def is_kcb_stock(code):
    try: 
        return str(code).zfill(6).startswith('688')
    except: 
        return False

def is_bse_stock(code):
    try: 
        return str(code).zfill(6).startswith('8')
    except: 
        return False

def is_st_stock(name):
    try: 
        return 'ST' in name or '退' in name or '*' in name or 'N' in name
    except: 
        return False

def calculate_relative_strength(stock_code, benchmark_code='000001', days=60):
    try:
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days+30)).strftime('%Y%m%d')
        
        stock_data = cache.get_history_data(stock_code, start_date, end_date)
        benchmark_data = cache.get_history_data(benchmark_code, start_date, end_date)
        
        if stock_data is None or benchmark_data is None or len(stock_data) < days or len(benchmark_data) < days:
            return 0
        
        stock_return = (stock_data['close'].iloc[-1] / stock_data['close'].iloc[-days] - 1) * 100
        benchmark_return = (benchmark_data['close'].iloc[-1] / benchmark_data['close'].iloc[-days] - 1) * 100
        
        return stock_return - benchmark_return
    except:
        return 0

# ==========================================
# 2. 大盘分析模块
# ==========================================
class MarketAnalyzer:
    """大盘分析器，判断市场环境"""
    
    def __init__(self):
        self.market_data = None
        self.market_status = "unknown"
        self.market_trend = 0
        self.market_volume_status = "normal"
        self.last_update = None
        
    def update_market_data(self, days=180):
        """更新大盘数据"""
        try:
            end_date = datetime.datetime.now().strftime('%Y%m%d')
            start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y%m%d')
            
            self.market_data = cache.get_history_data('000001', start_date, end_date)
            if self.market_data is None or len(self.market_data) < 60:
                return False
            
            self.last_update = datetime.datetime.now()
            return True
        except:
            return False
    
    def analyze_market(self):
        """分析市场状态"""
        if self.market_data is None or len(self.market_data) < 60:
            return "unknown", 0, "normal"
        
        close = self.market_data['close']
        volume = self.market_data['volume'] if 'volume' in self.market_data.columns else None
        
        current_price = close.iloc[-1]
        
        # 1. 趋势判断
        ma20 = close.rolling(20).mean().iloc[-1]
        ma60 = close.rolling(60).mean().iloc[-1]
        ma120 = close.rolling(120).mean().iloc[-1]
        
        # 计算各周期涨幅
        五日涨幅 = (current_price / close.iloc[-5] - 1) * 100 if len(close) > 5 else 0
        二十日涨幅 = (current_price / close.iloc[-20] - 1) * 100 if len(close) > 20 else 0
        六十日涨幅 = (current_price / close.iloc[-60] - 1) * 100 if len(close) > 60 else 0
        
        # 趋势评分
        trend_score = 0
        if current_price > ma20:
            trend_score += 1
        if current_price > ma60:
            trend_score += 1
        if current_price > ma120:
            trend_score += 1
        if ma20 > ma60:
            trend_score += 1
        if ma60 > ma120:
            trend_score += 1
        
        # 判断市场状态
        if trend_score >= 4 and 二十日涨幅 > 3 and 六十日涨幅 > 5:
            self.market_status = "bull"  # 牛市
        elif trend_score <= 1 and 二十日涨幅 < -3 and 六十日涨幅 < -5:
            self.market_status = "bear"  # 熊市
        else:
            self.market_status = "sideways"  # 震荡市
        
        # 2. 量能分析
        if volume is not None:
            vol_ma5 = volume.rolling(5).mean()
            vol_ma20 = volume.rolling(20).mean()
            
            current_vol = volume.iloc[-1]
            
            if current_vol > vol_ma5.iloc[-1] * 1.5:
                self.market_volume_status = "放量"
            elif current_vol < vol_ma5.iloc[-1] * 0.7:
                self.market_volume_status = "缩量"
            else:
                self.market_volume_status = "正常"
        
        # 3. 计算市场强度（用于进攻策略开关）
        self.market_trend = trend_score
        
        return self.market_status, self.market_trend, self.market_volume_status
    
    def should_use_aggressive(self):
        """判断是否应该使用进攻策略"""
        if self.market_status == "bull":
            return True, "牛市行情，适合进攻"
        elif self.market_status == "sideways" and self.market_trend >= 3:
            return True, "震荡市但趋势偏强，可适度进攻"
        else:
            return False, f"市场偏弱({self.market_status})，建议保守"

# ==========================================
# 3. Base Stock Engine
# ==========================================
class StockEngine:
    def __init__(self, exclude_gem=True):
        self.exclude_gem = exclude_gem
        self.realtime_data = None
        self.market_analyzer = MarketAnalyzer()
        
    def initialize_data(self, force_refresh=False):
        self.realtime_data = cache.get_realtime_data(force_refresh)
        if self.realtime_data is None:
            return False
        
        self.realtime_data = clean_data(self.realtime_data)
        
        # 更新大盘数据
        self.market_analyzer.update_market_data()
        market_status, trend, volume = self.market_analyzer.analyze_market()
        
        status_map = {'bull': '牛市', 'bear': '熊市', 'sideways': '震荡市'}
        print(f"\n【📊 当前市场状态】{status_map.get(market_status, '未知')} | 趋势强度:{trend}/5 | 量能:{volume}")
        
        if self.exclude_gem:
            self.realtime_data['is_gem'] = self.realtime_data['code'].apply(is_gem_stock)
            self.realtime_data['is_kcb'] = self.realtime_data['code'].apply(is_kcb_stock)
            self.realtime_data['is_bse'] = self.realtime_data['code'].apply(is_bse_stock)
            self.realtime_data = self.realtime_data[~self.realtime_data['is_gem'] & 
                                                    ~self.realtime_data['is_kcb'] & 
                                                    ~self.realtime_data['is_bse']].copy()
            
            total = len(self.realtime_data)
            print(f"\n【📌 板块分布】主板: {total} 只")
        
        return True
    
    def batch_analyze_stocks(self, stock_list, strategy_type='conservative'):
        results = []
        codes_needed = []
        
        for _, stock in stock_list.iterrows():
            code = str(stock['code']).zfill(6)
            codes_needed.append(code)
        
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime('%Y%m%d')
        
        print(f"\n📦 批量获取 {len(codes_needed)} 只股票的历史数据...")
        history_dict = cache.batch_get_history_data(codes_needed, start_date, end_date)
        
        for _, stock in stock_list.iterrows():
            code = str(stock['code']).zfill(6)
            
            if code not in history_dict:
                continue
            
            kline = history_dict[code]
            
            if strategy_type == 'conservative':
                analysis_result = self._conservative_analysis(stock, kline)
            else:
                analysis_result = self._aggressive_analysis(stock, kline)
            
            if analysis_result:
                results.append(analysis_result)
        
        return results
    
    def _conservative_analysis(self, stock, kline):
        if kline is None or len(kline) < 60:
            return None
        
        close = kline['close']
        volume = kline['volume'] if 'volume' in kline.columns else pd.Series([0]*len(close))
        
        current_price = close.iloc[-1]
        
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma120 = close.rolling(120).mean()
        
        trend_score = 0
        if current_price > ma60.iloc[-1]:
            trend_score += 10
            if ma20.iloc[-1] > ma60.iloc[-1]:
                trend_score += 10
                if ma60.iloc[-1] > ma120.iloc[-1]:
                    trend_score += 10
        
        relative_strength = calculate_relative_strength(stock['code'])
        strength_score = 30 if relative_strength > 15 else (20 if relative_strength > 5 else (10 if relative_strength > 0 else 0))
        
        pe = stock['pe']
        valuation_score = 20 if pe < 20 else (15 if pe < 30 else (10 if pe < 40 else 5))
        
        vol_ma20 = volume.rolling(20).mean()
        vol_std = volume.rolling(20).std()
        vol_cv = vol_std / vol_ma20
        
        volume_score = 20 if vol_cv.iloc[-1] < 0.5 else (10 if vol_cv.iloc[-1] < 0.8 else 5)
        
        total_score = trend_score + strength_score + valuation_score + volume_score
        
        if total_score >= 60:
            return {
                'code': stock['code'],
                'name': stock['name'],
                'price': round(current_price, 2),
                'pe': round(stock['pe'], 2),
                'turnover': round(stock['turnover'], 2),
                'relative_strength': round(relative_strength, 2),
                'trend_score': trend_score,
                'strength_score': strength_score,
                'valuation_score': valuation_score,
                'total_score': total_score
            }
        return None
    
    def _aggressive_analysis(self, stock, kline):
        if kline is None or len(kline) < 30:
            return None
        
        close = kline['close']
        volume = kline['volume'] if 'volume' in kline.columns else pd.Series([0]*len(close))
        
        current_price = close.iloc[-1]
        
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        
        # 1. 强势特征 (25分)
        strength_score = 0
        if current_price > ma5.iloc[-1]:
            strength_score += 8
        if ma5.iloc[-1] > ma10.iloc[-1]:
            strength_score += 8
        if ma10.iloc[-1] > ma20.iloc[-1]:
            strength_score += 9
        
        # 2. 量能配合 (25分)
        vol_ma5 = volume.rolling(5).mean()
        vol_ma20 = volume.rolling(20).mean()
        
        volume_score = 0
        if volume.iloc[-1] > vol_ma5.iloc[-1] * 1.2:  # 放量20%
            volume_score += 13
        elif volume.iloc[-1] > vol_ma5.iloc[-1]:
            volume_score += 8
        
        if vol_ma5.iloc[-1] > vol_ma20.iloc[-1]:
            volume_score += 12
        
        # 3. 动量指标 (20分)
        if len(close) > 6:
            five_day_return = (current_price / close.iloc[-6] - 1) * 100
        else:
            five_day_return = 0
        
        momentum_score = 0
        if 3 < five_day_return < 15:  # 适度上涨
            momentum_score += 10
        
        # MACD
        exp1 = close.ewm(span=12).mean()
        exp2 = close.ewm(span=26).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9).mean()
        
        if macd.iloc[-1] > signal.iloc[-1]:
            momentum_score += 10
        
        # 4. 回调安全度 (15分)
        recent_high = close.iloc[-5:].max()
        pullback = (recent_high - current_price) / recent_high
        
        safety_score = 15 if 0.02 < pullback < 0.08 else (8 if pullback < 0.12 else 0)
        
        # 5. 大盘配合度 (15分)
        market_score = 0
        if self.market_analyzer.market_status == "bull":
            market_score = 15
        elif self.market_analyzer.market_status == "sideways" and self.market_analyzer.market_trend >= 3:
            market_score = 10
        elif self.market_analyzer.market_trend >= 2:
            market_score = 5
        
        total_score = strength_score + volume_score + momentum_score + safety_score + market_score
        
        if total_score >= 65:  # 提高门槛
            return {
                'code': stock['code'],
                'name': stock['name'],
                'price': round(current_price, 2),
                'pe': round(stock['pe'], 2),
                'turnover': round(stock['turnover'], 2),
                'five_day_return': round(five_day_return, 2),
                'strength_score': strength_score,
                'volume_score': volume_score,
                'momentum_score': momentum_score,
                'safety_score': safety_score,
                'market_score': market_score,
                'total_score': total_score
            }
        return None

# ==========================================
# 4. Conservative Strategy
# ==========================================
def run_conservative_scanner(engine, output_count=10):
    print("\n" + "="*70)
    print("【🛡️ 保守型选股扫描器】- 稳健为主，注重安全性")
    print("="*70)
    
    conservative_conditions = (
        (engine.realtime_data['close'].between(5, 80)) &
        (engine.realtime_data['turnover'].between(3, 20)) &
        (engine.realtime_data['pe'].between(5, 40)) &
        (engine.realtime_data['pct_chg'].between(0, 7)) &
        (~engine.realtime_data['name'].apply(is_st_stock))
    )
    
    conservative_pool = engine.realtime_data[conservative_conditions].copy()
    print(f"🔍 基础筛选后: {len(conservative_pool)} 只")
    
    if conservative_pool.empty:
        print("❌ 没有股票通过筛选")
        return pd.DataFrame()
    
    analysis_pool = conservative_pool.nlargest(60, 'turnover')
    
    print("\n⚙️ 保守型技术分析...")
    results = engine.batch_analyze_stocks(analysis_pool, 'conservative')
    
    if not results:
        print("\n❌ 没有股票入选")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values('total_score', ascending=False).head(output_count)
    
    display_df = result_df[['code', 'name', 'price', 'pe', 'turnover', 
                            'relative_strength', 'trend_score', 'strength_score', 
                            'valuation_score', 'total_score']].copy()
    display_df.columns = ['代码', '名称', '现价', '市盈率', '换手率%', 
                          '相对强度', '趋势分', '强度分', '估值分', '总分']
    
    print("\n" + "="*70)
    print(f"🛡️ 【保守型选股结果 - 前{output_count}名】")
    print("="*70)
    print(display_df.to_string(index=False))
    
    filename = f'conservative_selection_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    display_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n💾 结果已保存至: {filename}")
    
    return result_df

# ==========================================
# 5. Aggressive Strategy (大盘增强版)
# ==========================================
def run_aggressive_scanner(engine, output_count=10):
    print("\n" + "="*70)
    print("【🔥 进攻型选股扫描器（大盘增强版）】- 顺势而为")
    print("="*70)
    
    # 先判断大盘是否适合进攻
    should_use, reason = engine.market_analyzer.should_use_aggressive()
    print(f"【🎯 进攻策略判断】{reason}")
    
    if not should_use:
        print("⚠️ 当前市场环境不适合进攻策略，建议使用保守策略")
        user_input = input("是否仍然继续执行进攻策略？(y/n): ")
        if user_input.lower() != 'y':
            return pd.DataFrame()
    
    # 根据大盘强度动态调整参数
    market_trend = engine.market_analyzer.market_trend
    
    if market_trend >= 4:
        # 强势市场，可以更激进
        min_turnover = 6
        max_pe = 90
        min_pct_chg = 1
    elif market_trend >= 3:
        # 中等市场，适度激进
        min_turnover = 5
        max_pe = 80
        min_pct_chg = 0
    else:
        # 弱势市场，保守进攻
        min_turnover = 4
        max_pe = 70
        min_pct_chg = -2
    
    aggressive_conditions = (
        (engine.realtime_data['close'] > 5) &
        (engine.realtime_data['close'] < 150) &
        (engine.realtime_data['turnover'] > min_turnover) &
        (engine.realtime_data['turnover'] < 35) &
        (engine.realtime_data['pe'] > 0) &
        (engine.realtime_data['pe'] < max_pe) &
        (engine.realtime_data['pct_chg'] > min_pct_chg) &
        (engine.realtime_data['pct_chg'] < 8) &
        (~engine.realtime_data['name'].apply(is_st_stock))
    )
    
    aggressive_pool = engine.realtime_data[aggressive_conditions].copy()
    print(f"🔍 基础筛选后: {len(aggressive_pool)} 只")
    
    if aggressive_pool.empty:
        print("❌ 没有股票通过筛选")
        return pd.DataFrame()
    
    analysis_pool = aggressive_pool.nlargest(100, 'turnover')
    
    print("\n⚙️ 进攻型技术分析（大盘增强版）...")
    results = engine.batch_analyze_stocks(analysis_pool, 'aggressive')
    
    if not results:
        print("\n❌ 没有股票入选")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values('total_score', ascending=False).head(output_count)
    
    display_df = result_df[['code', 'name', 'price', 'pe', 'turnover', 
                            'five_day_return', 'strength_score', 
                            'volume_score', 'momentum_score', 'safety_score', 'market_score', 'total_score']].copy()
    display_df.columns = ['代码', '名称', '现价', '市盈率', '换手率%', 
                          '五日涨幅%', '强势分', '量能分', '动量分', '安全分', '大盘分', '总分']
    
    print("\n" + "="*70)
    print(f"🔥 【进攻型选股结果 - 前{output_count}名】")
    print("="*70)
    print(display_df.to_string(index=False))
    
    filename = f'aggressive_selection_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    display_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n💾 结果已保存至: {filename}")
    
    return result_df

# ==========================================
# 6. Backtest Functions
# ==========================================
def run_conservative_backtest(code, name):
    print("\n" + "-"*60)
    print(f"【🛡️ 保守型回测】 {name}({code})")
    print("-"*60)
    
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=1095)).strftime('%Y%m%d')
    
    try:
        code = str(code).zfill(6)
        df = cache.get_history_data(code, start_date, end_date)
        if df is None or df.empty:
            print("❌ 无法获取数据")
            return None
        print(f"✅ 获取数据成功，共 {len(df)} 个交易日")
    except:
        print("❌ 获取数据失败")
        return None
    
    if len(df) < 60:
        return None
    
    close = df['close']
    
    df['ma20'] = close.rolling(20).mean()
    df['ma60'] = close.rolling(60).mean()
    
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    df['signal'] = 0
    
    for i in range(60, len(df)):
        buy_conditions = [
            df['close'].iloc[i] < df['ma20'].iloc[i] * 1.02,
            df['close'].iloc[i] > df['ma60'].iloc[i],
            df['rsi'].iloc[i] < 50,
            df['rsi'].iloc[i] > 30,
        ]
        
        sell_conditions = [
            df['close'].iloc[i] > df['ma20'].iloc[i] * 1.15,
            df['rsi'].iloc[i] > 70,
        ]
        
        if all(buy_conditions):
            df.loc[df.index[i], 'signal'] = 1
        elif any(sell_conditions):
            df.loc[df.index[i], 'signal'] = -1
    
    capital = 100000
    position = 0
    trades = []
    equity = [100000]
    
    for i in range(60, len(df)):
        price = df['close'].iloc[i]
        signal = df['signal'].iloc[i]
        
        if signal == 1 and position == 0:
            shares = int(capital * 0.7 / price)
            if shares > 0:
                cost = shares * price * 1.001
                capital -= cost
                position = shares
                trades.append({'action': 'BUY', 'price': price, 'shares': shares})
        
        elif signal == -1 and position > 0:
            value = position * price * 0.999
            capital += value
            trades.append({'action': 'SELL', 'price': price, 'shares': position})
            position = 0
        
        total = capital + (position * price if position > 0 else 0)
        equity.append(total)
    
    strategy_return = (equity[-1] / 100000 - 1) * 100
    benchmark_return = (df['close'].iloc[-1] / df['close'].iloc[60] - 1) * 100
    
    equity_series = pd.Series(equity)
    rolling_max = equity_series.cummax()
    drawdown = (rolling_max - equity_series) / rolling_max
    max_drawdown = drawdown.max() * 100
    
    buy_trades = [t for t in trades if t['action'] == 'BUY']
    sell_trades = [t for t in trades if t['action'] == 'SELL']
    
    wins = 0
    for i in range(min(len(buy_trades), len(sell_trades))):
        if sell_trades[i]['price'] > buy_trades[i]['price']:
            wins += 1
    win_rate = (wins / len(sell_trades) * 100) if sell_trades else 0
    
    print(f"\n【📈 保守型回测结果】")
    print(f"  策略收益: {strategy_return:.2f}%")
    print(f"  基准收益: {benchmark_return:.2f}%")
    print(f"  超额收益: {strategy_return - benchmark_return:.2f}%")
    print(f"  最大回撤: {max_drawdown:.2f}%")
    print(f"  交易次数: {len(buy_trades)}")
    print(f"  胜    率: {win_rate:.1f}%")
    
    return {
        'code': code, 'name': name,
        'strategy_return': round(strategy_return, 2),
        'benchmark_return': round(benchmark_return, 2),
        'excess_return': round(strategy_return - benchmark_return, 2),
        'max_drawdown': round(max_drawdown, 2),
        'win_rate': round(win_rate, 2),
        'trade_count': len(buy_trades)
    }

def run_aggressive_backtest(code, name, market_analyzer):
    """进攻型回测（大盘增强版）"""
    print("\n" + "-"*60)
    print(f"【🔥 进攻型回测（大盘增强版）】 {name}({code})")
    print("-"*60)
    
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime('%Y%m%d')
    
    try:
        code = str(code).zfill(6)
        df = cache.get_history_data(code, start_date, end_date)
        if df is None or df.empty:
            print("❌ 无法获取数据")
            return None
        print(f"✅ 获取数据成功，共 {len(df)} 个交易日")
    except:
        print("❌ 获取数据失败")
        return None
    
    if len(df) < 60:
        return None
    
    close = df['close']
    
    # 技术指标
    df['ma5'] = close.rolling(5).mean()
    df['ma10'] = close.rolling(10).mean()
    df['ma20'] = close.rolling(20).mean()
    df['ma60'] = close.rolling(60).mean()
    
    # 布林带
    df['bb_mid'] = close.rolling(20).mean()
    df['bb_std'] = close.rolling(20).std()
    df['bb_upper'] = df['bb_mid'] + 2 * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - 2 * df['bb_std']
    
    # RSI
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 成交量指标
    if 'volume' in df.columns:
        df['volume_ma5'] = df['volume'].rolling(5).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma5']
    
    # 回调深度
    df['recent_high'] = close.rolling(5).max()
    df['pullback'] = (df['recent_high'] - close) / df['recent_high']
    
    # 获取大盘历史数据用于回测
    market_data = cache.get_history_data('000001', start_date, end_date)
    
    # 策略信号
    df['signal'] = 0
    df['stop_loss'] = 0.0
    
    for i in range(30, len(df)):
        # 获取当时的大盘状态
        if market_data is not None and i < len(market_data):
            market_close = market_data['close'].iloc[i] if i < len(market_data) else 0
            market_ma20 = market_data['close'].rolling(20).mean().iloc[i] if i >= 20 else 0
            market_above_ma20 = market_close > market_ma20 if market_ma20 > 0 else False
        else:
            market_above_ma20 = False
        
        # 买入条件：强势股回调 + 大盘配合
        buy_conditions = [
            df['ma5'].iloc[i] > df['ma10'].iloc[i],
            df['ma10'].iloc[i] > df['ma20'].iloc[i],
            df['close'].iloc[i] > df['ma20'].iloc[i],
            0.02 < df['pullback'].iloc[i] < 0.10,
            df['rsi'].iloc[i] < 60,
            df['rsi'].iloc[i] > 40,
            market_above_ma20  # 大盘要在20日线上
        ]
        
        # 卖出条件
        sell_conditions = [
            df['close'].iloc[i] < df['ma20'].iloc[i] * 0.95,
            df['rsi'].iloc[i] > 75,
            not market_above_ma20,  # 大盘破位也要卖
        ]
        
        if all(buy_conditions):
            df.loc[df.index[i], 'signal'] = 1
            df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * 0.93
        elif any(sell_conditions):
            df.loc[df.index[i], 'signal'] = -1
    
    # 模拟交易
    capital = 100000
    position = 0
    trades = []
    equity = [100000]
    stop_price = 0
    
    for i in range(30, len(df)):
        price = df['close'].iloc[i]
        signal = df['signal'].iloc[i]
        
        # 检查止损
        if position > 0 and price < stop_price:
            value = position * price * 0.999
            capital += value
            trades.append({'action': 'SELL', 'price': price, 'shares': position, 'reason': 'stop_loss'})
            position = 0
            stop_price = 0
        
        # 买入
        if signal == 1 and position == 0:
            shares = int(capital * 0.7 / price)  # 降低到70%仓位
            if shares > 0:
                cost = shares * price * 1.001
                capital -= cost
                position = shares
                stop_price = df['stop_loss'].iloc[i]
                trades.append({'action': 'BUY', 'price': price, 'shares': shares, 'reason': 'signal'})
        
        # 卖出
        elif signal == -1 and position > 0:
            value = position * price * 0.999
            capital += value
            trades.append({'action': 'SELL', 'price': price, 'shares': position, 'reason': 'signal'})
            position = 0
            stop_price = 0
        
        total = capital + (position * price if position > 0 else 0)
        equity.append(total)
    
    # 计算收益
    strategy_return = (equity[-1] / 100000 - 1) * 100
    benchmark_return = (df['close'].iloc[-1] / df['close'].iloc[30] - 1) * 100
    
    # 最大回撤
    equity_series = pd.Series(equity)
    rolling_max = equity_series.cummax()
    drawdown = (rolling_max - equity_series) / rolling_max
    max_drawdown = drawdown.max() * 100
    
    # 胜率
    buy_trades = [t for t in trades if t['action'] == 'BUY']
    sell_trades = [t for t in trades if t['action'] == 'SELL']
    
    wins = 0
    for i in range(min(len(buy_trades), len(sell_trades))):
        if sell_trades[i]['price'] > buy_trades[i]['price']:
            wins += 1
    win_rate = (wins / len(sell_trades) * 100) if sell_trades else 0
    
    print(f"\n【📈 进攻型回测结果（大盘增强版）】")
    print(f"  策略收益: {strategy_return:.2f}%")
    print(f"  基准收益: {benchmark_return:.2f}%")
    print(f"  超额收益: {strategy_return - benchmark_return:.2f}%")
    print(f"  最大回撤: {max_drawdown:.2f}%")
    print(f"  交易次数: {len(buy_trades)}")
    print(f"  胜    率: {win_rate:.1f}%")
    
    return {
        'code': code, 'name': name,
        'strategy_return': round(strategy_return, 2),
        'benchmark_return': round(benchmark_return, 2),
        'excess_return': round(strategy_return - benchmark_return, 2),
        'max_drawdown': round(max_drawdown, 2),
        'win_rate': round(win_rate, 2),
        'trade_count': len(buy_trades)
    }

# ==========================================
# 7. Main Program
# ==========================================
if __name__ == "__main__":
    print("="*70)
    print("          智能选股系统 v10.0 - 大盘增强版")
    print("="*70)
    
    engine = StockEngine(exclude_gem=True)
    
    if not engine.initialize_data(force_refresh=False):
        print("❌ 数据初始化失败，程序退出")
        sys.exit(0)
    
    print("\n" + "="*70)
    print("请选择策略：")
    print("  1. 🛡️ 保守型策略（稳健为主）")
    print("  2. 🔥 进攻型策略（大盘增强版）")
    print("  3. ⚔️ 双策略对比")
    print("  0. ❌ 退出")
    print("="*70)
    
    choice = input("请输入数字 (0-3): ").strip()
    
    if choice == '0':
        print("👋 程序退出")
        sys.exit(0)
    
    if choice == '1':
        print("\n🛡️ 运行保守型策略...")
        selection_result = run_conservative_scanner(engine, 10)
        
        if not selection_result.empty:
            print("\n" + "="*70)
            print("【🔄 保守型回测验证】")
            print("="*70)
            
            backtest_results = []
            for idx, (_, stock) in enumerate(selection_result.iterrows()):
                print(f"\n[{idx+1}/10] 回测: {stock['name']}")
                result = run_conservative_backtest(stock['code'], stock['name'])
                if result:
                    backtest_results.append(result)
                time.sleep(1)
            
            if backtest_results:
                print("\n" + "="*70)
                print("【🛡️ 保守型回测汇总】")
                print("="*70)
                
                results_df = pd.DataFrame(backtest_results)
                results_df = results_df.sort_values('excess_return', ascending=False)
                
                display_df = results_df[['name', 'strategy_return', 'benchmark_return', 
                                         'excess_return', 'win_rate', 'max_drawdown']].copy()
                display_df.columns = ['名称', '策略收益', '基准收益', '超额收益', '胜率', '最大回撤']
                print(display_df.to_string(index=False))
                
                filename = f'conservative_backtest_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                results_df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"\n💾 结果已保存至: {filename}")
    
    elif choice == '2':
        print("\n🔥 运行进攻型策略（大盘增强版）...")
        selection_result = run_aggressive_scanner(engine, 10)
        
        if not selection_result.empty:
            print("\n" + "="*70)
            print("【🔄 进攻型回测验证（大盘增强版）】")
            print("="*70)
            
            backtest_results = []
            for idx, (_, stock) in enumerate(selection_result.iterrows()):
                print(f"\n[{idx+1}/10] 回测: {stock['name']}")
                result = run_aggressive_backtest(stock['code'], stock['name'], engine.market_analyzer)
                if result:
                    backtest_results.append(result)
                time.sleep(1)
            
            if backtest_results:
                print("\n" + "="*70)
                print("【🔥 进攻型回测汇总（大盘增强版）】")
                print("="*70)
                
                results_df = pd.DataFrame(backtest_results)
                results_df = results_df.sort_values('excess_return', ascending=False)
                
                display_df = results_df[['name', 'strategy_return', 'benchmark_return', 
                                         'excess_return', 'win_rate', 'max_drawdown']].copy()
                display_df.columns = ['名称', '策略收益', '基准收益', '超额收益', '胜率', '最大回撤']
                print(display_df.to_string(index=False))
                
                filename = f'aggressive_backtest_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                results_df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"\n💾 结果已保存至: {filename}")
    
    elif choice == '3':
        print("\n⚔️ 运行双策略对比...")
        
        print("\n" + "="*70)
        print("【第一部分：保守型策略】")
        conservative_result = run_conservative_scanner(engine, 5)
        
        print("\n" + "="*70)
        print("【第二部分：进攻型策略（大盘增强版）】")
        aggressive_result = run_aggressive_scanner(engine, 5)
        
        all_results = []
        
        if not conservative_result.empty:
            for _, stock in conservative_result.iterrows():
                print(f"\n【保守型】回测: {stock['name']}")
                result = run_conservative_backtest(stock['code'], stock['name'])
                if result:
                    result['strategy_type'] = 'conservative'
                    all_results.append(result)
                time.sleep(1)
        
        if not aggressive_result.empty:
            for _, stock in aggressive_result.iterrows():
                print(f"\n【进攻型】回测: {stock['name']}")
                result = run_aggressive_backtest(stock['code'], stock['name'], engine.market_analyzer)
                if result:
                    result['strategy_type'] = 'aggressive'
                    all_results.append(result)
                time.sleep(1)
        
        if all_results:
            print("\n" + "="*70)
            print("【⚔️ 双策略对比汇总】")
            print("="*70)
            
            results_df = pd.DataFrame(all_results)
            type_map = {'conservative': '保守型', 'aggressive': '进攻型'}
            results_df['strategy_type_cn'] = results_df['strategy_type'].map(type_map)
            
            display_df = results_df[['strategy_type_cn', 'name', 'strategy_return', 
                                     'benchmark_return', 'excess_return', 'win_rate', 'max_drawdown']].copy()
            display_df.columns = ['策略类型', '名称', '策略收益', '基准收益', '超额收益', '胜率', '最大回撤']
            display_df = display_df.sort_values('超额收益', ascending=False)
            print(display_df.to_string(index=False))
            
            filename = f'dual_strategy_comparison_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
            results_df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n💾 对比结果已保存至: {filename}")
    
    else:
        print("❌ 输入错误，程序退出")
    
    cache.clear_expired_cache()
    
    print("\n" + "="*70)
    print("✨ 程序执行完成！")
    print("="*70)
