import qstock as qs
import pandas as pd
import numpy as np
import datetime
import time
import warnings
warnings.filterwarnings('ignore')

class EnhancedStockScanner:
    def __init__(self):
        self.today = datetime.datetime.now()
        self.market_status = self._check_market_status()
        
    def _check_market_status(self):
        """检查当前市场状态"""
        try:
            # 获取大盘指数
            index_data = qs.get_data('000001', start=(self.today - datetime.timedelta(days=30)).strftime('%Y%m%d'))
            if index_data is not None and len(index_data) > 5:
                # 计算大盘趋势
                ma5 = index_data['close'].rolling(5).mean().iloc[-1]
                ma20 = index_data['close'].rolling(20).mean().iloc[-1]
                current = index_data['close'].iloc[-1]
                
                if current > ma20 and ma20 > ma5:
                    return '牛市'
                elif current < ma20 and ma20 < ma5:
                    return '熊市'
                else:
                    return '震荡市'
        except:
            return '未知'
        return '未知'
    
    def _is_gem_stock(self, code):
        """判断是否为创业板股票"""
        try:
            code_str = str(code).zfill(6)
            # 创业板代码以 300 开头
            return code_str.startswith('300')
        except:
            return False
    
    def _is_kcb_stock(self, code):
        """判断是否为科创板股票（代码以688开头）"""
        try:
            code_str = str(code).zfill(6)
            # 科创板代码以 688 开头
            return code_str.startswith('688')
        except:
            return False
    
    def _is_be_stock(self, code):
        """判断是否为北交所股票（代码以8开头）"""
        try:
            code_str = str(code).zfill(6)
            # 北交所代码以 8 开头
            return code_str.startswith('8')
        except:
            return False
    
    def _safe_get_data(self, code, start, end):
        """安全获取数据的方法 - 修复日期格式问题"""
        try:
            # 确保 start 是字符串格式
            if hasattr(start, 'strftime'):
                start = start.strftime('%Y%m%d')
            elif isinstance(start, str):
                start = start.replace('-', '')
            else:
                start = str(start)
            
            # 确保 end 是字符串格式
            if hasattr(end, 'strftime'):
                end = end.strftime('%Y%m%d')
            elif isinstance(end, str):
                end = end.replace('-', '')
            else:
                end = str(end)
            
            return qs.get_data(code, start=start, end=end)
        except Exception as e:
            print(f"获取数据失败 {code}: {e}")
            return None
    
    def get_reliable_data(self, max_retries=3):
        """增强的数据获取函数"""
        for i in range(max_retries):
            try:
                df = qs.realtime_data()
                if df is not None and len(df) > 100:  # 确保数据量合理
                    return df
            except Exception as e:
                if i < max_retries - 1:
                    print(f"数据获取失败，第{i+1}次重试...")
                    time.sleep(2)
                else:
                    print(f"实时数据获取失败: {e}")
        
        # 降级方案：获取昨日数据
        print("使用备用数据源...")
        yesterday = (self.today - datetime.timedelta(days=1)).strftime('%Y%m%d')
        try:
            # 尝试获取所有股票的历史数据
            return qs.get_data('all', end=yesterday)
        except:
            print("备用数据源也失败")
            return None
    
    def _clean_data(self, df):
        """数据清洗"""
        if df is None or df.empty:
            return df
            
        rename_map = {
            '代码': 'code', '名称': 'name', 
            '最新': 'close', '最新价': 'close', 
            '涨幅': 'pct_change', '涨跌幅': 'pct_change',
            '换手率': 'turnover', '换手': 'turnover',
            '市盈率': 'pe', '市盈率(动)': 'pe',
            '成交量': 'volume', '成交额': 'amount',
            '流通市值': 'float_mv', '总市值': 'total_mv'
        }
        
        # 只重命名存在的列
        existing_cols = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=existing_cols)
        
        # 强制转换为数值
        numeric_cols = ['close', 'pe', 'turnover', 'volume', 'amount', 'float_mv', 'total_mv']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df.dropna(subset=['close', 'pe', 'turnover'] if all(c in df.columns for c in ['close', 'pe', 'turnover']) else df.columns)
    
    def calculate_technical_score(self, kline):
        """综合技术指标评分"""
        if kline is None or len(kline) < 60:
            return 0
        
        score = 0
        close = kline['close']
        
        # 1. 多均线系统 (30分)
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma120 = close.rolling(120).mean()
        
        current = close.iloc[-1]
        last_ma20 = ma20.iloc[-1] if not pd.isna(ma20.iloc[-1]) else current
        last_ma60 = ma60.iloc[-1] if not pd.isna(ma60.iloc[-1]) else current
        last_ma120 = ma120.iloc[-1] if not pd.isna(ma120.iloc[-1]) else current
        
        # 多头排列加分
        if current > last_ma20 > last_ma60 > last_ma120:
            score += 30
        elif current > last_ma20 > last_ma60:
            score += 20
        elif current > last_ma20:
            score += 10
            
        # 2. 相对强弱 (20分)
        try:
            end_date = kline.index[-1].strftime('%Y%m%d') if hasattr(kline.index[-1], 'strftime') else str(kline.index[-1])
            start_date = kline.index[0].strftime('%Y%m%d') if hasattr(kline.index[0], 'strftime') else str(kline.index[0])
            
            index_data = self._safe_get_data('000001', start_date, end_date)
            if index_data is not None and len(index_data) > 20:
                stock_ret = close.pct_change().iloc[-20:].mean() * 100
                index_ret = index_data['close'].pct_change().iloc[-20:].mean() * 100
                if stock_ret > index_ret:
                    score += 20 if stock_ret > index_ret * 2 else 10
        except:
            pass
            
        # 3. 成交量验证 (20分)
        if 'volume' in kline.columns:
            volume = kline['volume']
            vol_ma5 = volume.rolling(5).mean()
            vol_ma20 = volume.rolling(20).mean()
            
            if volume.iloc[-1] > vol_ma5.iloc[-1] * 1.2:
                score += 10
            if vol_ma5.iloc[-1] > vol_ma20.iloc[-1]:
                score += 10
            
        # 4. MACD状态 (15分)
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        
        if not pd.isna(macd.iloc[-1]) and not pd.isna(signal.iloc[-1]) and macd.iloc[-1] > signal.iloc[-1]:
            score += 15
            
        # 5. RSI不超买 (15分)
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        if not pd.isna(rsi.iloc[-1]) and 40 < rsi.iloc[-1] < 70:
            score += 15
            
        return score
    
    def enhanced_scanner(self, limit=10):
        """增强版选股器 - 默认选10支"""
        print("="*60)
        print(f"【增强版选股器启动】 市场状态: {self.market_status}")
        print(f"【目标】选出 {limit} 支非创业板股票")
        print("="*60)
        
        # 1. 获取数据
        df = self.get_reliable_data()
        if df is None or df.empty:
            print("无法获取数据，程序退出")
            return pd.DataFrame()
        
        # 2. 数据清洗
        df = self._clean_data(df)
        print(f"原始数据: {len(df)} 只股票")
        
        # 3. 确保必要列存在
        required_cols = ['close', 'pe', 'turnover', 'name', 'code']
        if not all(col in df.columns for col in required_cols):
            print(f"缺少必要列: {set(required_cols) - set(df.columns)}")
            return pd.DataFrame()
        
        # 4. 剔除创业板股票
        df['is_gem'] = df['code'].apply(self._is_gem_stock)
        df['is_kcb'] = df['code'].apply(self._is_kcb_stock)
        df['is_be'] = df['code'].apply(self._is_be_stock)
        
        # 统计各板块数量
        gem_count = df['is_gem'].sum()
        kcb_count = df['is_kcb'].sum()
        be_count = df['is_be'].sum()
        main_board_count = len(df) - gem_count - kcb_count - be_count
        
        print(f"\n【板块分布】")
        print(f"  主板: {main_board_count} 只")
        print(f"  创业板: {gem_count} 只 (已剔除)")
        print(f"  科创板: {kcb_count} 只 (已剔除)")
        print(f"  北交所: {be_count} 只 (已剔除)")
        
        # 过滤掉创业板、科创板、北交所
        df = df[~df['is_gem'] & ~df['is_kcb'] & ~df['is_be']].copy()
        print(f"\n剔除后剩余: {len(df)} 只主板股票")
        
        if df.empty:
            print("没有主板股票可供选择")
            return pd.DataFrame()
        
        # 5. 多维度筛选
        # 基础筛选
        base_condition = (
            (df['close'] > 2) &  # 避免低价股
            (df['close'] < 200) &  # 避免高价股
            (df['turnover'] > 3) &  # 活跃度要求
            (df['turnover'] < 30) &  # 避免过度投机
            (df['pe'] > 0) & 
            (df['pe'] < 50) & 
            (~df['name'].str.contains('ST|退市|\\*|N', na=False))
        )
        
        pool = df[base_condition].copy()
        print(f"基础筛选后: {len(pool)} 只")
        
        if pool.empty:
            print("没有股票通过基础筛选")
            return pd.DataFrame()
        
        # 6. 技术面评分
        print("\n正在进行技术面评分...")
        end_date = self.today
        start_date = self.today - datetime.timedelta(days=150)
        
        scores = []
        total = len(pool)
        for idx, (_, row) in enumerate(pool.iterrows()):
            try:
                code = str(row['code']).zfill(6)
                # 使用安全方法获取数据
                kline = self._safe_get_data(code, start_date, end_date)
                
                if kline is not None and len(kline) >= 60:
                    score = self.calculate_technical_score(kline)
                else:
                    score = 0
                scores.append(score)
                
                # 显示进度
                if (idx + 1) % 20 == 0:
                    print(f"  进度: {idx + 1}/{total}")
                    
            except Exception as e:
                print(f"处理 {code} 时出错: {e}")
                scores.append(0)
                
        pool['tech_score'] = scores
        
        # 7. 综合排序
        # 归一化处理
        max_pe = pool['pe'].max()
        max_turnover = pool['turnover'].max()
        
        pool['composite_score'] = (
            pool['tech_score'] * 0.5 +
            (100 / pool['pe'].clip(lower=1)) * 0.2 * (100/max_pe) +  # PE越低越好
            pool['turnover'] * 0.3 * (100/max_turnover)  # 换手率越高越好
        )
        
        # 按综合评分排序
        final_pool = pool.nlargest(limit * 2, 'composite_score')
        
        # 8. 输出结果
        print("\n" + "="*60)
        print(f"【最终选股结果 - 前{limit}名】")
        print("="*60)
        
        result = []
        for _, row in final_pool.head(limit).iterrows():
            result.append({
                '代码': str(row['code']).zfill(6),
                '名称': row['name'],
                '现价': round(row['close'], 2),
                'PE': round(row['pe'], 2),
                '换手率%': round(row['turnover'], 2),
                '技术评分': row['tech_score'],
                '综合评分': round(row['composite_score'], 2)
            })
        
        result_df = pd.DataFrame(result)
        if not result_df.empty:
            print(result_df.to_string(index=False))
            
            # 保存到CSV文件
            result_df.to_csv(f'selected_stocks_{self.today.strftime("%Y%m%d")}.csv', index=False, encoding='utf-8-sig')
            print(f"\n结果已保存到: selected_stocks_{self.today.strftime('%Y%m%d')}.csv")
        else:
            print("没有符合条件的股票")
        
        return result_df
    
    def advanced_backtest(self, code, name):
        """增强版回测（包含交易成本和止损）"""
        print("\n" + "-"*50)
        print(f"【回测】 {name}({code})")
        print("-"*50)
        
        # 获取3年数据
        end_date = self.today
        start_date = self.today - datetime.timedelta(days=1095)
        
        try:
            code = str(code).zfill(6)
            # 使用安全获取数据的方法
            df = self._safe_get_data(code, start_date, end_date)
            
            if df is None or df.empty:
                print(f"  无法获取{name}的历史数据")
                return None
                
            print(f"  数据量: {len(df)} 条")
            
        except Exception as e:
            print(f"  获取{name}历史数据失败: {e}")
            return None
        
        # 计算技术指标
        df = self._calculate_indicators(df)
        
        # 生成交易信号（多条件过滤）
        df['signal'] = self._generate_signals(df)
        
        # 模拟交易（含止损和交易成本）
        portfolio = self._simulate_trading(df)
        
        # 输出简要结果
        self._print_short_backtest_results(portfolio, df, name)
        
        return portfolio
    
    def _calculate_indicators(self, df):
        """计算技术指标"""
        close = df['close']
        
        # 均线
        df['ma20'] = close.rolling(20).mean()
        df['ma60'] = close.rolling(60).mean()
        df['ma120'] = close.rolling(120).mean()
        
        # MACD
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['signal_line'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['signal_line']
        
        # RSI
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 成交量指标
        if 'volume' in df.columns:
            df['volume_ma5'] = df['volume'].rolling(5).mean()
            df['volume_ratio'] = df['volume'] / df['volume_ma5']
        
        # ATR（用于止损）
        if all(col in df.columns for col in ['high', 'low']):
            high_low = df['high'] - df['low']
            high_close = abs(df['high'] - df['close'].shift())
            low_close = abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            df['atr'] = true_range.rolling(14).mean()
        
        return df
    
    def _generate_signals(self, df):
        """生成交易信号（多条件）"""
        signals = [0] * len(df)
        
        for i in range(20, len(df)):
            # 买入条件
            buy_conditions = [
                df['close'].iloc[i] > df['ma20'].iloc[i],  # 站上20日线
                df['ma20'].iloc[i] > df['ma60'].iloc[i],  # 短期均线多头
            ]
            
            # RSI条件
            if 'rsi' in df.columns and not pd.isna(df['rsi'].iloc[i]):
                buy_conditions.append(40 < df['rsi'].iloc[i] < 70)
            
            # 成交量条件
            if 'volume_ratio' in df.columns and not pd.isna(df['volume_ratio'].iloc[i]):
                buy_conditions.append(df['volume_ratio'].iloc[i] > 1.2)
            
            # MACD条件
            if 'macd_hist' in df.columns and i > 0 and not pd.isna(df['macd_hist'].iloc[i]) and not pd.isna(df['macd_hist'].iloc[i-1]):
                buy_conditions.append(df['macd_hist'].iloc[i] > df['macd_hist'].iloc[i-1])
            
            # 卖出条件
            sell_conditions = [
                df['close'].iloc[i] < df['ma20'].iloc[i],  # 跌破20日线
            ]
            
            if 'rsi' in df.columns and not pd.isna(df['rsi'].iloc[i]):
                sell_conditions.append(df['rsi'].iloc[i] > 80)  # 超买
            
            if all(buy_conditions):
                signals[i] = 1
            elif any(sell_conditions):
                signals[i] = 0
            else:
                signals[i] = signals[i-1] if i > 0 else 0
                
        return signals
    
    def _simulate_trading(self, df):
        """模拟交易（含成本和止损）"""
        capital = 100000  # 初始资金
        position = 0
        trades = []
        
        # 交易成本参数
        commission_rate = 0.0003  # 万三佣金
        slippage = 0.001  # 千一滑点
        stop_loss = 0.07  # 7%止损
        
        entry_price = 0
        
        for i in range(1, len(df)):
            date = df.index[i]
            price = df['close'].iloc[i]
            signal = df['signal'].iloc[i]
            
            # 检查止损
            if position > 0 and entry_price > 0:
                if price < entry_price * (1 - stop_loss):
                    # 触发止损
                    exit_value = position * price * (1 - slippage)
                    commission = exit_value * commission_rate
                    capital += exit_value - commission
                    
                    trades.append({
                        'date': date,
                        'action': 'SELL',
                        'price': price,
                        'shares': position,
                        'value': exit_value,
                        'reason': 'stop_loss'
                    })
                    position = 0
                    entry_price = 0
                    continue
            
            # 买入信号
            if signal == 1 and position == 0:
                shares = int(capital / price * 0.95)  # 只用95%资金
                if shares > 0:
                    cost = shares * price * (1 + slippage)
                    commission = cost * commission_rate
                    total_cost = cost + commission
                    
                    if total_cost <= capital:
                        position = shares
                        capital -= total_cost
                        entry_price = price
                        
                        trades.append({
                            'date': date,
                            'action': 'BUY',
                            'price': price,
                            'shares': shares,
                            'value': total_cost,
                            'reason': 'signal'
                        })
            
            # 卖出信号
            elif signal == 0 and position > 0:
                exit_value = position * price * (1 - slippage)
                commission = exit_value * commission_rate
                capital += exit_value - commission
                
                trades.append({
                    'date': date,
                    'action': 'SELL',
                    'price': price,
                    'shares': position,
                    'value': exit_value,
                    'reason': 'signal'
                })
                position = 0
                entry_price = 0
        
        # 计算最终收益
        final_value = capital + (position * df['close'].iloc[-1] if position > 0 else 0)
        
        # 计算交易统计
        buy_trades = [t for t in trades if t['action'] == 'BUY']
        sell_trades = [t for t in trades if t['action'] == 'SELL']
        
        return {
            'trades': trades,
            'initial_capital': 100000,
            'final_value': final_value,
            'total_return': (final_value / 100000 - 1) * 100,
            'trades_count': len(buy_trades),
            'buy_trades': buy_trades,
            'sell_trades': sell_trades
        }
    
    def _print_short_backtest_results(self, portfolio, df, name):
        """打印简短的背测结果"""
        benchmark_return = (df['close'].iloc[-1] / df['close'].iloc[0] - 1) * 100
        
        print(f"  {name}: 策略收益 {portfolio['total_return']:.2f}% | 基准收益 {benchmark_return:.2f}% | 交易次数 {portfolio['trades_count']}")
        
        # 根据表现添加标记
        if portfolio['total_return'] > benchmark_return * 1.2:
            print(f"  ✅ 显著跑赢")
        elif portfolio['total_return'] > benchmark_return:
            print(f"  👍 跑赢基准")
        elif portfolio['total_return'] > 0:
            print(f"  👌 盈利但未跑赢")
        else:
            print(f"  ❌ 亏损")

# ==========================================
# 主程序
# ==========================================
if __name__ == "__main__":
    import sys
    
    print("="*60)
    print("股票量化选股系统 v3.0 - 主板精选")
    print("="*60)
    print("【选股规则】")
    print("  ✅ 只选主板股票（剔除创业板300/科创板688/北交所8）")
    print("  ✅ 技术面+基本面综合评分")
    print("  ✅ 输出前10名并进行回测验证")
    print("="*60)
    
    # 创建扫描器实例
    scanner = EnhancedStockScanner()
    
    # 执行选股 - 选出10支
    candidates = scanner.enhanced_scanner(limit=10)
    
    # 对选出的所有股票进行回测
    if not candidates.empty:
        print("\n" + "="*60)
        print("【开始回测验证 - 所有选出的10支股票】")
        print("="*60)
        
        backtest_results = []
        
        for idx, (_, stock) in enumerate(candidates.iterrows()):
            print(f"\n[{idx+1}/10] 正在回测: {stock['名称']} ({stock['代码']})")
            result = scanner.advanced_backtest(stock['代码'], stock['名称'])
            
            if result:
                # 获取基准收益用于比较
                end_date = scanner.today
                start_date = scanner.today - datetime.timedelta(days=1095)
                df = scanner._safe_get_data(str(stock['代码']).zfill(6), start_date, end_date)
                if df is not None and not df.empty:
                    benchmark = (df['close'].iloc[-1] / df['close'].iloc[0] - 1) * 100
                else:
                    benchmark = 0
                
                backtest_results.append({
                    '代码': stock['代码'],
                    '名称': stock['名称'],
                    '策略收益%': round(result['total_return'], 2),
                    '基准收益%': round(benchmark, 2),
                    '超额收益%': round(result['total_return'] - benchmark, 2),
                    '交易次数': result['trades_count']
                })
            
            # 每个股票之间暂停一下，避免请求过快
            if idx < 9:
                time.sleep(1)
        
        # 输出回测汇总
        if backtest_results:
            print("\n" + "="*60)
            print("【回测结果汇总】")
            print("="*60)
            
            results_df = pd.DataFrame(backtest_results)
            results_df = results_df.sort_values('策略收益%', ascending=False)
            print(results_df.to_string(index=False))
            
            # 保存汇总结果
            results_df.to_csv(f'backtest_summary_{scanner.today.strftime("%Y%m%d")}.csv', index=False, encoding='utf-8-sig')
            print(f"\n回测汇总已保存到: backtest_summary_{scanner.today.strftime('%Y%m%d')}.csv")
            
            # 统计表现
            win_count = len(results_df[results_df['策略收益%'] > 0])
            beat_count = len(results_df[results_df['超额收益%'] > 0])
            
            print("\n【整体统计】")
            print(f"  盈利股票: {win_count}/10 ({win_count*10}%)")
            print(f"  跑赢基准: {beat_count}/10 ({beat_count*10}%)")
            print(f"  平均策略收益: {results_df['策略收益%'].mean():.2f}%")
            print(f"  平均基准收益: {results_df['基准收益%'].mean():.2f}%")
    else:
        print("\n没有选出符合条件的股票，程序退出")
        sys.exit(0)
    
    print("\n" + "="*60)
    print("程序执行完成！")
    print("="*60)
