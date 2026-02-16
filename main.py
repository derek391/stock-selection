import qstock as qs
import pandas as pd
import numpy as np
import datetime
import time
import warnings
import sys

# 忽略警告，保持输出整洁
warnings.filterwarnings('ignore')
# 设置中文对齐
pd.set_option('display.unicode.east_asian_width', True)

# ==========================================
# 1. 辅助函数：数据清洗
# ==========================================
def 清洗数据(df):
    """将中文列名映射为英文，便于后续处理"""
    列名映射 = {
        '代码': 'code', '名称': 'name', 
        '最新': 'close', '最新价': 'close', 
        '涨幅': 'pct_chg', '涨跌幅': 'pct_chg',
        '换手率': 'turnover', '换手': 'turnover',
        '市盈率': 'pe', '市盈率(动)': 'pe', 
        '成交量': 'volume', '成交额': 'amount',
        '量比': 'vol_ratio', '流通市值': 'float_mv',
        '总市值': 'total_mv', '振幅': 'amplitude'
    }
    
    # 只重命名存在的列
    存在的列 = {k: v for k, v in 列名映射.items() if k in df.columns}
    df = df.rename(columns=存在的列)
    
    # 转换数值列
    数值列 = ['close', 'pe', 'turnover', 'pct_chg', 'volume', 'amount', 'float_mv', 'total_mv', 'amplitude']
    for col in 数值列:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    return df

# ==========================================
# 2. 辅助函数：板块判断
# ==========================================
def 是创业板(code):
    """判断是否为创业板股票（300开头）"""
    try:
        return str(code).zfill(6).startswith('300')
    except:
        return False

def 是科创板(code):
    """判断是否为科创板股票（688开头）"""
    try:
        return str(code).zfill(6).startswith('688')
    except:
        return False

def 是北交所(code):
    """判断是否为北交所股票（8开头）"""
    try:
        return str(code).zfill(6).startswith('8')
    except:
        return False

def 是ST股票(名称):
    """判断是否为ST股票"""
    try:
        return 'ST' in 名称 or '退' in 名称 or '*' in 名称 or 'N' in 名称
    except:
        return False

# ==========================================
# 3. 执行时间优化函数
# ==========================================
def 应该执行选股(强制执行=False):
    """
    判断今天是否适合执行选股
    参数:
        强制执行: 是否忽略时间建议强制运行
    """
    today = datetime.datetime.now()
    month = today.month
    hour = today.hour
    minute = today.minute
    weekday = today.weekday()  # 0=周一, 6=周日
    
    print(f"\n【⏰ 时间检查】当前时间: {today.strftime('%Y-%m-%d %H:%M')}")
    
    # 周末检查
    if weekday >= 5:  # 周六、周日
        print("【❌ 提示】今天是周末，A股休市，无需运行选股")
        return False
    
    # 交易时间检查
    is_morning = (hour == 9 and minute >= 30) or (10 <= hour < 11) or (hour == 11 and minute <= 30)
    is_afternoon = (hour == 13) or (hour == 14) or (hour == 15 and minute == 0)
    
    if not (is_morning or is_afternoon):
        print("【⚠️ 提示】当前不是交易时间，建议在交易时段运行")
        if not 强制执行:
            return False
    
    # 最佳选股时间：下午2:30后
    if hour < 14 or (hour == 14 and minute < 30):
        print("【💡 建议】下午2:30后执行选股效果最佳，尾盘信号更可靠")
        if not 强制执行:
            return False
    else:
        print("【✅ 时间合适】正处于尾盘选股黄金窗口")
    
    # 月份判断：根据A股日历效应
    high_priority_months = [2, 3, 11]  # 2月、3月、11月是黄金期
    medium_priority_months = [5, 7, 8, 9, 12]  # 正常期
    low_priority_months = [1, 4, 6, 10]  # 雷区期，谨慎
    
    if month in high_priority_months:
        print(f"【📈 月份提示】{month}月是题材炒作黄金期，适合积极选股")
        return True
    elif month in medium_priority_months:
        print(f"【📊 月份提示】{month}月适合正常选股")
        return True
    else:
        print(f"【⚠️ 月份警告】{month}月是业绩雷区，建议谨慎操作")
        return 强制执行

# ==========================================
# 4. 改进版选股策略
# ==========================================
def 计算相对强度(股票代码, 基准代码='000001', 天数=60):
    """计算股票相对于大盘的强度"""
    try:
        结束日期 = datetime.datetime.now().strftime('%Y%m%d')
        开始日期 = (datetime.datetime.now() - datetime.timedelta(days=天数+30)).strftime('%Y%m%d')
        
        股票数据 = qs.get_data(股票代码, start=开始日期, end=结束日期)
        大盘数据 = qs.get_data(基准代码, start=开始日期, end=结束日期)
        
        if 股票数据 is None or 大盘数据 is None or len(股票数据) < 天数 or len(大盘数据) < 天数:
            return 0
        
        股票收益 = (股票数据['close'].iloc[-1] / 股票数据['close'].iloc[-天数] - 1) * 100
        大盘收益 = (大盘数据['close'].iloc[-1] / 大盘数据['close'].iloc[-天数] - 1) * 100
        
        return 股票收益 - 大盘收益
    except:
        return 0

def 运行改进版扫描器(输出数量=10, 剔除创业板=True):
    """
    改进版选股扫描器
    """
    print("\n" + "="*70)
    print("【📡 改进版选股扫描器】")
    print("="*70)
    
    # --- A. 获取实时数据 ---
    try:
        print("\n正在获取实时行情数据...")
        df = qs.realtime_data()
        print(f"✅ 获取成功，共 {len(df)} 只股票")
    except Exception as e:
        print(f"❌ 【错误】获取行情数据失败: {e}")
        return pd.DataFrame()
    
    # --- B. 数据清洗 ---
    df = 清洗数据(df)
    
    # 检查必要列
    必要列 = ['code', 'name', 'close', 'turnover', 'pe', 'pct_chg']
    for col in 必要列:
        if col not in df.columns:
            print(f"❌ 缺少必要列: {col}")
            return pd.DataFrame()
    
    df = df.dropna(subset=['close', 'turnover', 'pe'])
    
    # --- C. 板块过滤 ---
    if 剔除创业板:
        df['是创业板'] = df['code'].apply(是创业板)
        df['是科创板'] = df['code'].apply(是科创板)
        df['是北交所'] = df['code'].apply(是北交所)
        
        创业板数量 = df['是创业板'].sum()
        科创板数量 = df['是科创板'].sum()
        北交所数量 = df['是北交所'].sum()
        
        print(f"\n【板块分布】")
        print(f"  主板: {len(df) - 创业板数量 - 科创板数量 - 北交所数量} 只")
        print(f"  创业板: {创业板数量} 只 (将被剔除)")
        print(f"  科创板: {科创板数量} 只 (将被剔除)")
        print(f"  北交所: {北交所数量} 只 (将被剔除)")
        
        df = df[~df['是创业板'] & ~df['是科创板'] & ~df['是北交所']].copy()
        print(f"\n✅ 剔除后剩余: {len(df)} 只主板股票")
    
    # --- D. 改进的筛选条件 ---
    # 1. 股价在5-100元之间（剔除仙股和过高价股）
    # 2. 换手率 5-25%（活跃但不能过度投机）
    # 3. PE 10-50（估值合理）
    # 4. 当日涨幅 1-8%（有上涨动能但没涨停）
    # 5. 非ST股
    基础条件 = (
        (df['close'].between(5, 100)) &
        (df['turnover'].between(5, 25)) &
        (df['pe'].between(10, 50)) &
        (df['pct_chg'].between(1, 8)) &
        (~df['name'].apply(是ST股票))
    )
    
    候选池 = df[基础条件].copy()
    print(f"\n🔍 基础筛选后: {len(候选池)} 只")
    
    if 候选池.empty:
        print("❌ 没有股票通过基础筛选")
        return pd.DataFrame()
    
    # 取前50只进行分析
    分析池 = 候选池.nlargest(50, 'turnover')
    
    # --- E. 深度技术分析 ---
    print("\n⚙️ 正在进行深度技术分析...")
    
    入选股票 = []
    
    for idx, (_, 股票) in enumerate(分析池.iterrows()):
        代码 = str(股票['code']).zfill(6)
        名称 = 股票['name']
        
        if (idx + 1) % 10 == 0:
            print(f"  进度: {idx + 1}/{len(分析池)}")
        
        try:
            time.sleep(0.05)
            
            # 获取历史数据
            结束日期 = datetime.datetime.now().strftime('%Y%m%d')
            开始日期 = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime('%Y%m%d')
            k线 = qs.get_data(代码, start=开始日期, end=结束日期)
            
            if k线 is None or len(k线) < 60:
                continue
            
            # 计算技术指标
            close = k线['close']
            volume = k线['volume'] if 'volume' in k线.columns else pd.Series([0]*len(close))
            
            # 均线
            ma5 = close.rolling(5).mean()
            ma10 = close.rolling(10).mean()
            ma20 = close.rolling(20).mean()
            ma60 = close.rolling(60).mean()
            
            当前价 = close.iloc[-1]
            
            # 1. 趋势强度 (30分)
            趋势分 = 0
            if 当前价 > ma20.iloc[-1] and ma20.iloc[-1] > ma60.iloc[-1]:
                趋势分 += 20
                if 当前价 > ma10.iloc[-1] and ma10.iloc[-1] > ma20.iloc[-1]:
                    趋势分 += 10
            
            # 2. 相对强度 (20分)
            相对强度 = 计算相对强度(代码)
            强度分 = 20 if 相对强度 > 10 else (10 if 相对强度 > 0 else 0)
            
            # 3. 成交量健康度 (20分)
            vol_ma5 = volume.rolling(5).mean()
            vol_ma20 = volume.rolling(20).mean()
            
            量能分 = 0
            if volume.iloc[-1] > vol_ma5.iloc[-1] * 1.2:
                量能分 += 10
            if vol_ma5.iloc[-1] > vol_ma20.iloc[-1]:
                量能分 += 10
            
            # 4. 动量指标 (20分)
            # MACD
            exp1 = close.ewm(span=12).mean()
            exp2 = close.ewm(span=26).mean()
            macd = exp1 - exp2
            signal = macd.ewm(span=9).mean()
            
            # RSI
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            动量分 = 0
            if macd.iloc[-1] > signal.iloc[-1]:
                动量分 += 10
            if 40 < rsi.iloc[-1] < 70:
                动量分 += 10
            
            # 5. 稳定性评分 (10分)
            # 计算最近20日的波动率
            波动率 = close.pct_change().rolling(20).std().iloc[-1] * 100
            稳定分 = 10 if 波动率 < 3 else (5 if 波动率 < 5 else 0)
            
            # 总分
            总分 = 趋势分 + 强度分 + 量能分 + 动量分 + 稳定分
            
            if 总分 >= 60:
                print(f"  ✅ 入选: {名称:<8} 价格:{当前价:<6.2f} 总分:{总分} 强度:{相对强度:.1f}")
                
                入选股票.append({
                    '代码': 代码,
                    '名称': 名称,
                    '现价': round(当前价, 2),
                    '市盈率': round(股票['pe'], 2),
                    '换手率%': round(股票['turnover'], 2),
                    '相对强度': round(相对强度, 2),
                    '趋势分': 趋势分,
                    '量能分': 量能分,
                    '总分': 总分
                })
                
        except Exception as e:
            continue
    
    # --- F. 输出结果 ---
    最终结果 = pd.DataFrame(入选股票)
    
    if not 最终结果.empty:
        最终结果 = 最终结果.sort_values('总分', ascending=False).head(输出数量)
        
        print("\n" + "="*70)
        print(f"🏆 【改进版选股结果 - 前{输出数量}名】")
        print("="*70)
        print(最终结果.to_string(index=False))
        
        文件名 = f'改进版选股_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        最终结果.to_csv(文件名, index=False, encoding='utf-8-sig')
        print(f"\n💾 结果已保存至: {文件名}")
        
        return 最终结果
    else:
        print("\n❌ 没有股票通过技术分析")
        return pd.DataFrame()

# ==========================================
# 5. 改进版回测策略
# ==========================================
def 运行改进版回测(代码, 名称):
    """
    改进版回测策略 - 趋势跟踪 + 止损 + 仓位管理
    """
    print("\n" + "-"*60)
    print(f"【📊 改进版回测】 {名称}({代码})")
    print("-"*60)
    
    # 获取3年数据
    结束日期 = datetime.datetime.now().strftime('%Y%m%d')
    开始日期 = (datetime.datetime.now() - datetime.timedelta(days=1095)).strftime('%Y%m%d')
    
    try:
        代码 = str(代码).zfill(6)
        df = qs.get_data(代码, start=开始日期, end=结束日期)
        
        if df is None or df.empty:
            print("❌ 无法获取历史数据")
            return None
            
        print(f"✅ 获取数据成功，共 {len(df)} 个交易日")
        
    except Exception as e:
        print(f"❌ 获取数据失败: {e}")
        return None
    
    if len(df) < 60:
        print("⚠️ 数据不足")
        return None
    
    # 计算技术指标
    close = df['close']
    
    # 多重均线
    df['ma5'] = close.rolling(5).mean()
    df['ma10'] = close.rolling(10).mean()
    df['ma20'] = close.rolling(20).mean()
    df['ma60'] = close.rolling(60).mean()
    
    # MACD
    exp1 = close.ewm(span=12).mean()
    exp2 = close.ewm(span=26).mean()
    df['macd'] = exp1 - exp2
    df['signal'] = df['macd'].ewm(span=9).mean()
    df['macd_hist'] = df['macd'] - df['signal']
    
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
    
    # ATR用于动态止损
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()
    
    # --- 改进的交易信号 ---
    df['signal'] = 0
    
    for i in range(60, len(df)):
        # 买入条件（多头排列 + MACD金叉 + RSI适中）
        buy_conditions = [
            df['close'].iloc[i] > df['ma20'].iloc[i],
            df['ma5'].iloc[i] > df['ma10'].iloc[i],
            df['ma10'].iloc[i] > df['ma20'].iloc[i],
            df['ma20'].iloc[i] > df['ma60'].iloc[i],
            df['macd'].iloc[i] > df['signal'].iloc[i],
            df['macd_hist'].iloc[i] > df['macd_hist'].iloc[i-1],
            40 < df['rsi'].iloc[i] < 70
        ]
        
        # 卖出条件
        sell_conditions = [
            df['close'].iloc[i] < df['ma20'].iloc[i] * 0.97,  # 跌破3%
            df['rsi'].iloc[i] > 80,  # 超买
            df['macd_hist'].iloc[i] < df['macd_hist'].iloc[i-1] * 0.5  # 红柱大幅缩短
        ]
        
        if all(buy_conditions):
            df.loc[df.index[i], 'signal'] = 1
        elif any(sell_conditions):
            df.loc[df.index[i], 'signal'] = -1
    
    # --- 模拟交易 ---
    capital = 100000
    position = 0
    trades = []
    equity_curve = []
    
    for i in range(60, len(df)):
        date = df.index[i]
        price = df['close'].iloc[i]
        signal = df['signal'].iloc[i]
        atr = df['atr'].iloc[i]
        
        # 动态止损价
        stop_loss = price - 2 * atr if position > 0 else 0
        
        # 检查止损
        if position > 0 and price < stop_loss:
            # 止损卖出
            exit_value = position * price * 0.999  # 扣滑点
            capital += exit_value
            trades.append({
                'date': date, 'action': 'SELL', 'price': price,
                'shares': position, 'reason': 'stop_loss'
            })
            position = 0
        
        # 买入信号
        elif signal == 1 and position == 0:
            # 根据ATR动态仓位
            risk_per_trade = capital * 0.02  # 每次承担2%风险
            position_size = risk_per_trade / (2 * atr)
            shares = int(position_size)
            
            if shares > 0 and shares * price <= capital:
                cost = shares * price * 1.001  # 加滑点
                capital -= cost
                position = shares
                trades.append({
                    'date': date, 'action': 'BUY', 'price': price,
                    'shares': shares, 'reason': 'signal'
                })
        
        # 卖出信号
        elif signal == -1 and position > 0:
            exit_value = position * price * 0.999
            capital += exit_value
            trades.append({
                'date': date, 'action': 'SELL', 'price': price,
                'shares': position, 'reason': 'signal'
            })
            position = 0
        
        # 记录净值
        total_value = capital + (position * price if position > 0 else 0)
        equity_curve.append({'date': date, 'equity': total_value})
    
    # 计算收益
    equity_df = pd.DataFrame(equity_curve)
    if not equity_df.empty:
        equity_df.set_index('date', inplace=True)
        df = df.join(equity_df, how='left')
        df['equity'].fillna(method='ffill', inplace=True)
        df['equity'].fillna(100000, inplace=True)
        
        # 计算收益率
        df['strategy_return'] = df['equity'].pct_change()
        df['benchmark_return'] = df['close'].pct_change()
        
        strategy_total = (df['equity'].iloc[-1] / 100000 - 1) * 100
        benchmark_total = (df['close'].iloc[-1] / df['close'].iloc[60] - 1) * 100
        
        # 计算最大回撤
        cumulative = (1 + df['strategy_return'].fillna(0)).cumprod()
        running_max = cumulative.cummax()
        drawdown = (running_max - cumulative) / running_max
        max_drawdown = drawdown.max() * 100
        
        # 计算胜率
        buy_trades = [t for t in trades if t['action'] == 'BUY']
        sell_trades = [t for t in trades if t['action'] == 'SELL']
        
        wins = 0
        for i in range(min(len(buy_trades), len(sell_trades))):
            if sell_trades[i]['price'] > buy_trades[i]['price']:
                wins += 1
        win_rate = (wins / len(sell_trades) * 100) if sell_trades else 0
        
        print(f"\n【📈 回测结果】")
        print(f"  策略收益: {strategy_total:.2f}%")
        print(f"  基准收益: {benchmark_total:.2f}%")
        print(f"  超额收益: {strategy_total - benchmark_total:.2f}%")
        print(f"  最大回撤: {max_drawdown:.2f}%")
        print(f"  交易次数: {len(buy_trades)}")
        print(f"  胜    率: {win_rate:.1f}%")
        
        # 结论
        print(f"\n【结论】", end=" ")
        if strategy_total > benchmark_total:
            print("✅ 跑赢大盘")
            if strategy_total > 0:
                print("  盈利策略")
        elif strategy_total > 0:
            print("⚠️ 盈利但跑输大盘")
        else:
            print("❌ 亏损策略")
        
        return {
            '代码': 代码, '名称': 名称,
            '策略收益': round(strategy_total, 2),
            '基准收益': round(benchmark_total, 2),
            '超额收益': round(strategy_total - benchmark_total, 2),
            '最大回撤': round(max_drawdown, 2),
            '胜率': round(win_rate, 2),
            '交易次数': len(buy_trades)
        }
    
    return None

# ==========================================
# 6. 主程序
# ==========================================
if __name__ == "__main__":
    print("="*70)
    print("          改进版智能选股回测系统 v5.0")
    print("="*70)
    print("【✨ 改进功能】")
    print("  ✓ 相对强度选股（对比大盘）")
    print("  ✓ 动态仓位管理（基于ATR）")
    print("  ✓ 多重均线系统（5/10/20/60）")
    print("  ✓ MACD+RSI双指标确认")
    print("  ✓ 动态止损（2倍ATR）")
    print("="*70)
    
    # 执行时间检查
    if not 应该执行选股(强制执行=False):
        print("\n⚠️ 当前时段选股效果可能不佳")
        用户输入 = input("是否仍然继续执行？(y/n): ")
        if 用户输入.lower() != 'y':
            print("👋 程序退出")
            sys.exit(0)
    
    # 运行改进版选股
    选股结果 = 运行改进版扫描器(输出数量=10, 剔除创业板=True)
    
    # 回测验证
    if not 选股结果.empty:
        print("\n" + "="*70)
        print("【🔄 开始回测验证】")
        print("="*70)
        
        回测汇总 = []
        
        for idx, (_, 股票) in enumerate(选股结果.iterrows()):
            print(f"\n[{idx+1}/10] 回测: {股票['名称']}")
            回测结果 = 运行改进版回测(股票['代码'], 股票['名称'])
            
            if 回测结果:
                回测汇总.append(回测结果)
            
            time.sleep(1)
        
        if 回测汇总:
            print("\n" + "="*70)
            print("【📊 回测汇总】")
            print("="*70)
            
            汇总df = pd.DataFrame(回测汇总)
            汇总df = 汇总df.sort_values('超额收益', ascending=False)
            
            显示列 = ['名称', '策略收益', '基准收益', '超额收益', '胜率', '最大回撤']
            print(汇总df[显示列].to_string(index=False))
            
            # 保存
            文件名 = f'改进版回测_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
            汇总df.to_csv(文件名, index=False, encoding='utf-8-sig')
            print(f"\n💾 回测汇总已保存至: {文件名}")
            
            # 统计
            盈利数 = (汇总df['策略收益'] > 0).sum()
            跑赢数 = (汇总df['超额收益'] > 0).sum()
            print(f"\n【统计】盈利:{盈利数}/10 跑赢:{跑赢数}/10")
    else:
        print("\n❌ 没有选出股票")
        sys.exit(0)
