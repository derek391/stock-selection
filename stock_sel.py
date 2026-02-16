import qstock as qs
import pandas as pd
import numpy as np
import datetime
import warnings

# 忽略 pandas 的一些警告，保持输出整洁
warnings.filterwarnings('ignore')

# ==========================================
# 辅助函数：清洗列名
# ==========================================
def clean_data(df):
    # 中文列名映射为英文，方便后续操作
    rename_map = {
        '代码': 'code', '名称': 'name', 
        '最新': 'close', '最新价': 'close', 
        '涨幅': 'pct_change', '涨跌幅': 'pct_change',
        '换手率': 'turnover', '换手': 'turnover',
        '市盈率': 'pe', '市盈率(动)': 'pe', '市盈率-动态': 'pe',
        '成交量': 'volume'
    }
    df = df.rename(columns=rename_map)
    
    # 强制转数字，无法转换的变为空值
    for col in ['close', 'pe', 'turnover']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    return df

# ==========================================
# 第一部分：选股策略 (Scanner)
# ==========================================
def run_scanner(limit=5):
    print("------------------------------------------------")
    print("【Step 1】开始全市场扫描选股...")
    
    # 1. 获取全市场实时数据
    try:
        df = qs.realtime_data()
    except Exception as e:
        print(f"获取数据失败: {e}")
        return pd.DataFrame()
    
    # 2. 清洗数据
    df = clean_data(df)
    print(f"数据获取成功，共 {len(df)} 只股票。")
    
    # 3. 第一轮筛选：基本面与活跃度 (Basic Filter)
    # ----------------------------------------------------
    # 逻辑：
    # a. 有收盘价
    # b. 换手率 > 5% (提高门槛，只看非常活跃的)
    # c. 0 < PE < 60 (剔除亏损和高估值)
    # d. 剔除ST
    
    # 确保没有空值
    df = df.dropna(subset=['close', 'turnover', 'pe'])
    
    condition = (
        (df['close'] > 0) & 
        (df['turnover'] > 5) & 
        (df['pe'] > 0) & 
        (df['pe'] < 60) & 
        (~df['name'].str.contains('ST'))
    )
    
    pool = df[condition].copy()
    
    # 按换手率倒序排列，取前 10 只进入决赛圈
    pool = pool.sort_values(by='turnover', ascending=False).head(10)
    
    print("\n-------- 第一轮筛选结果 (活跃 + 基本面) --------")
    if pool.empty:
        print("没有股票通过第一轮筛选。")
        return pd.DataFrame()
    else:
        # 打印这一轮幸存者的名字
        print(f"入围 {len(pool)} 只: {pool['name'].tolist()}")
        # 打印详细一点的信息
        print(pool[['code', 'name', 'close', 'pe', 'turnover']].to_string(index=False))

    print("\n-------- 第二轮筛选：趋势核查 (60日均线) --------")
    
    selected_list = []
    
    # 准备日期范围：取过去 150 天的数据，确保能算出 60日均线
    end_date_str = datetime.datetime.now().strftime('%Y%m%d')
    start_date_str = (datetime.datetime.now() - datetime.timedelta(days=150)).strftime('%Y%m%d')
    
    for idx, row in pool.iterrows():
        code = row['code']
        name = row['name']
        
        try:
            # 补全代码为6位字符串
            code_str = str(code).zfill(6)
            
            # 【修复点】去掉 rows 参数，使用 start 和 end
            kline = qs.get_data(code_str, start=start_date_str, end=end_date_str)
            
            if kline is None or len(kline) < 60: 
                print(f"  [跳过] {name}: 上市时间不足或数据获取失败")
                continue
            
            # 计算60日均线
            ma60 = kline['close'].rolling(60).mean().iloc[-1]
            current_price = kline['close'].iloc[-1]
            
            # 打印调试信息
            is_trend_up = current_price > ma60
            status = "通过 ✅" if is_trend_up else "淘汰 ❌ (股价在60日线下方)"
            print(f"  检查 {name} ({code_str}): 现价={current_price:.2f}, 60日线={ma60:.2f} -> {status}")
            
            if is_trend_up:
                # 计算乖离率
                bias = (current_price - ma60) / ma60 * 100
                selected_list.append({
                    '代码': code_str,
                    '名称': name,
                    '现价': current_price,
                    'PE': row['pe'],
                    '换手率%': row['turnover'],
                    '60日线乖离%': round(bias, 2)
                })
                
        except Exception as e:
            print(f"  [错误] 检查 {name} 时出错: {e}")
            continue
            
    final_df = pd.DataFrame(selected_list)
    
    if not final_df.empty:
        # 按乖离率排序
        final_df = final_df.sort_values(by='60日线乖离%', ascending=False).head(limit)
        print("\n======== 最终选股结果 (Top Candidates) ========")
        print(final_df.to_string(index=False))
        return final_df
    else:
        print("\n结果: 虽然有活跃股，但全都在下跌趋势中 (均未通过60日线筛选)。")
        return pd.DataFrame()

# ==========================================
# 第二部分：回测引擎 (验证选出来的第一名)
# ==========================================
def run_backtest(code, name):
    print("\n------------------------------------------------")
    print(f"【Step 2】对第一名 [{name} ({code})] 进行历史验证...")
    
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y%m%d')
    
    try:
        # 补全代码
        code = str(code).zfill(6)
        df = qs.get_data(code, start=start_date, end=end_date)
    except:
        print("无法获取历史数据。")
        return
        
    if df is None or df.empty:
        print("历史数据为空。")
        return

    # 策略：简单的 20日均线 趋势跟踪
    df['ma20'] = df['close'].rolling(20).mean()
    
    # 信号：收盘价 > 20日线 = 持有(1)，否则空仓(0)
    df['signal'] = np.where(df['close'] > df['ma20'], 1, 0)
    df['position'] = df['signal'].shift(1).fillna(0)
    
    # 收益计算
    df['pct_change'] = df['close'].pct_change()
    df['strategy_ret'] = df['position'] * df['pct_change']
    
    df['cum_market'] = (1 + df['pct_change']).cumprod()
    df['cum_strategy'] = (1 + df['strategy_ret']).cumprod()
    
    total_ret = df['cum_strategy'].iloc[-1] - 1
    market_ret = df['cum_market'].iloc[-1] - 1
    
    print(f"回测区间: {start_date} 至 {end_date}")
    print(f"策略累计收益: {total_ret*100:.2f}%")
    print(f"同期基准收益: {market_ret*100:.2f}%")
    
    if total_ret > 0:
        if total_ret > market_ret:
            print(">> 结论: 该股趋势性强，跑赢大盘，建议关注！✅")
        else:
            print(">> 结论: 策略盈利，但没跑赢持有不动。")
    else:
        print(">> 结论: 策略亏损，趋势不明显，建议换股。❌")

# ==========================================
# 主程序
# ==========================================
if __name__ == "__main__":
    candidates = run_scanner(limit=5)
    
    if not candidates.empty:
        # 自动取第一名进行回测
        top_stock = candidates.iloc[0]
        run_backtest(top_stock['代码'], top_stock['名称'])
