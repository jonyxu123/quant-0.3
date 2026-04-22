"""
策略权重配置模块 - 30套策略(A-AD)的因子权重配置
基于《多因子选股系统_v8.0》第5章策略组合定义

策略版本历史:
- v6.0: A-H (8套基础策略)
- v7.0: I-P (8套扩展策略)
- v7.1: Q-W (7套大师策略)
- v8.0: X-AD (7套高级策略)
"""

# ============================================================
# 30套策略权重配置 STRATEGY_COMBOS
# ============================================================
STRATEGY_COMBOS = {
    # ===== v6.0 基础策略 A-H =====
    'A': {
        'name': '价值防御型',
        'regime': '利率上行期、市场风险偏好低、防御性行情',
        'weights': {
            'V_EP': 0.20, 'V_BP': 0.15, 'V_FCFY': 0.15, 'P_ROE': 0.15,
            'Q_OCF_NI': 0.10, 'S_LN_FLOAT': -0.10, 'M_MOM_12_1': 0.10,
            'M_RSI_OVERSOLD': -0.05
        },
        'hard_filter': {}
    },
    'B': {
        'name': '成长进攻型',
        'regime': '流动性宽松、市场风险偏好高、成长风格占优',
        'weights': {
            'G_NI_YOY': 0.20, 'G_REV_YOY': 0.15, 'E_SUE': 0.15, 'Q_GPMD': 0.10,
            'Q_ATD': 0.10, 'M_MOM_12_1': 0.15, 'M_RS_6M': 0.10, 'S_AMT_20_Z': 0.05
        },
        'hard_filter': {}
    },
    'C': {
        'name': '质量优先型',
        'regime': '任何市场环境，尤其适合中长期持有',
        'weights': {
            'P_ROE': 0.18, 'P_ROA': 0.12, 'P_GPM': 0.12, 'P_CFOA': 0.12,
            'Q_OCF_NI': 0.10, 'Q_APR': -0.10, 'L_CCR': 0.10,
            'E_EARNINGS_MOM': 0.08, 'M_MOM_12_1': 0.08
        },
        'hard_filter': {}
    },
    'D': {
        'name': '均衡综合型',
        'regime': '不确定市场风格切换时，作为底仓配置',
        'weights': {
            'V_EP': 0.12, 'P_ROE': 0.12, 'G_NI_YOY': 0.12, 'M_MOM_12_1': 0.10,
            'M_RS_3M': 0.08, 'P_CFOA': 0.10, 'S_LN_FLOAT': -0.10,
            'C_WINNER_RATE': 0.08, 'E_SUE': 0.08, 'S_AMT_20_Z': 0.10
        },
        'hard_filter': {}
    },
    'E': {
        'name': '小盘成长增强型',
        'regime': '流动性充裕、小盘风格强势期',
        'weights': {
            'S_LN_FLOAT': -0.25, 'G_NI_YOY': 0.18, 'E_SUE': 0.15, 'G_QPT': 0.10,
            'Q_GPMD': 0.10, 'I_FUND_NEW': 0.08, 'S_AMT_20_Z': 0.08, 'Q_APR': -0.06
        },
        'hard_filter': {}
    },
    'F': {
        'name': '主升浪启动型',
        'regime': '牛市主升浪阶段，趋势确认+量价配合',
        'weights': {
            'M_NEW_HIGH_20': 0.15, 'M_MA_BULL': 0.15, 'M_VOL_ACCEL': 0.12,
            'M_RS_3M': 0.10, 'M_RSI_50_70': 0.10, 'M_MACD_GOLDEN': 0.10,
            'MF_F_OUT_IN_RATIO': 0.08, 'G_NI_YOY': 0.08, 'P_ROE': 0.07,
            'M_RSI_OVERSOLD': -0.05
        },
        'hard_filter': {
            'M_MA_BULL': {'op': '==', 'value': True},
            'M_VOL_ACCEL': {'op': '>=', 'value': 1.5},
            'G_NI_YOY': {'op': '>', 'value': 15}
        }
    },
    'G': {
        'name': '超跌反弹型',
        'regime': '市场超跌后反弹确认期',
        'weights': {
            'M_MOM_12_1': -0.18, 'M_RSI_OVERSOLD': 0.15, 'MF_VOL_SHRINK': 0.12,
            'V_EP': 0.12, 'V_BP': -0.08, 'L_DEBT_RATIO': -0.08,
            'S_TURNOVER': 0.07, 'Q_OCF_NI': 0.05, 'C_CONCENTRATION': -0.05,
            'TP_MACD_DIV_BOT': 0.10
        },
        'hard_filter': {}
    },
    'H': {
        'name': '综合趋势确认型',
        'regime': '趋势明确且持续的市场环境',
        'weights': {
            'M_MOM_12_1': 0.30, 'M_MA_BULL': 0.15, 'M_MACD_GOLDEN': 0.10,
            'M_VOL_ACCEL': 0.10, 'M_RS_3M': 0.08, 'M_INDU_MOM': 0.07,
            'P_ROE': 0.10, 'V_EP': 0.10
        },
        'hard_filter': {}
    },

    # ===== v7.0 扩展策略 I-P =====
    'I': {
        'name': '机构抱团型',
        'regime': '机构资金集中持仓的优质标的跟踪',
        'weights': {
            'I_FUND_HOLD_PCT': 0.20, 'I_FUND_CONSENSUS': 0.18, 'I_FUND_NEW': 0.15,
            'P_ROE': 0.15, 'G_NI_YOY': 0.12, 'M_MOM_6M': 0.10, 'S_AMT_20_Z': 0.10
        },
        'hard_filter': {
            'I_FUND_COUNT': {'op': '>', 'value': 10},
            'P_ROE': {'op': '>', 'value': 12},
            'M_MOM_6M': {'op': '>', 'value': 0}
        }
    },
    'J': {
        'name': '高股息收息型',
        'regime': '追求稳定现金流收益的保守型投资者',
        'weights': {
            'V_DY': 0.25, 'V_EP': 0.15, 'P_CFOA': 0.15, 'Q_OCF_NI': 0.12,
            'L_DEBT_RATIO': -0.10, 'P_ROE': 0.10, 'G_NI_YOY': 0.08,
            'M_MOM_12_1': 0.05
        },
        'hard_filter': {
            'V_DY': {'op': '>', 'value': 3},
            'V_PE_TTM': {'op': '>', 'value': 0}
        }
    },
    'K': {
        'name': '筹码集中突破型',
        'regime': '筹码高度集中且即将突破的个股',
        'weights': {
            'C_CONCENTRATION': -0.20, 'C_WINNER_RATE': 0.15, 'M_NEW_HIGH_20': 0.15,
            'M_VOL_ACCEL': 0.12, 'M_MA_BULL': 0.10, 'MF_MAIN_NET_IN': 0.10,
            'G_NI_YOY': 0.10, 'S_TURNOVER': 0.08
        },
        'hard_filter': {
            'C_CONCENTRATION': {'op': '<', 'value': 15},
            'C_WINNER_RATE': {'op': '>', 'value': 70},
            'M_VOL_ACCEL': {'op': '>', 'value': 1.5}
        }
    },
    'L': {
        'name': '盈利预测上调型',
        'regime': '券商连续上调盈利预期的个股',
        'weights': {
            'E_EARNINGS_MOM': 0.25, 'E_SUE': 0.20, 'G_NI_YOY': 0.15,
            'G_REV_YOY': 0.10, 'Q_GPMD': 0.10, 'M_MOM_3M': 0.10,
            'I_FUND_COUNT': 0.10
        },
        'hard_filter': {
            'E_EARNINGS_MOM': {'op': '>', 'value': 5},
            'E_SUE': {'op': '>', 'value': 0},
            'I_FUND_COUNT': {'op': '>', 'value': 5}
        }
    },
    'M': {
        'name': '事件驱动机会型',
        'regime': '有明确事件催化剂的个股（业绩预增、股东增持、龙虎榜机构买入等）',
        'weights': {
            'EV_EARNING_ALERT': 0.20, 'EV_LHB_NET_BUY': 0.18,
            'EV_SHHOLDER_INCREASE': 0.15, 'EV_BLOCK_TRADE_PREM': 0.10,
            'EV_MANAGEMENT_BUY': 0.10, 'M_MOM_3M': 0.12,
            'G_NI_YOY': 0.08, 'S_AMT_20_Z': 0.07
        },
        'hard_filter': {
            'EV_EARNING_ALERT': {'op': '>', 'value': 30},
            'M_MOM_3M': {'op': '>', 'value': 0}
        }
    },
    'N': {
        'name': '北向资金聪明钱型',
        'regime': '北向资金大幅加仓+连续增持标的，叠加基本面质量过滤',
        'weights': {
            'MF_NORTH_NET_IN_20D': 0.22, 'MF_NORTH_HOLD_CHANGE': 0.18,
            'I_NORTH_HOLD_PCT': 0.12, 'P_ROE': 0.14, 'V_EP': 0.10,
            'M_RS_6M': 0.10, 'S_AMT_20_Z': 0.08, 'Q_OCF_NI': 0.06
        },
        'hard_filter': {
            'MF_NORTH_NET_IN_20D': {'op': '>', 'value': 5000},
            'P_ROE': {'op': '>', 'value': 10},
            'V_PE_TTM': {'op': '>', 'value': 0},
            'V_PE_TTM_upper': {'op': '<', 'value': 50}
        }
    },
    'O': {
        'name': '行业轮动动量型',
        'regime': '市场风格切换期，选择强势行业中的龙头个股',
        'weights': {
            'M_INDU_MOM': 0.20, 'MF_SECTOR_FLOW_IN': 0.16,
            'M_RS_3M': 0.14, 'M_NEW_HIGH_60': 0.12, 'M_MA_BULL': 0.10,
            'M_VOL_ACCEL': 0.10, 'G_REV_YOY': 0.10, 'S_LN_FLOAT': -0.08
        },
        'hard_filter': {
            'M_VOL_ACCEL': {'op': '>', 'value': 1.2}
        }
    },
    'P': {
        'name': '低波红利防御增强型',
        'regime': '追求稳定收益+回撤控制的防御配置，适合熊市和震荡市',
        'weights': {
            'V_DY': 0.24, 'V_EP': 0.14, 'TP_VOL_COMPRESS': 0.14,
            'P_CFOA': 0.12, 'Q_OCF_NI': 0.10, 'L_DEBT_RATIO': -0.10,
            'P_ROE': 0.08, 'M_MOM_12_1': 0.08
        },
        'hard_filter': {
            'V_DY': {'op': '>', 'value': 2.5},
            'V_PE_TTM': {'op': '>', 'value': 0}
        }
    },

    # ===== v7.1 大师策略 Q-W =====
    'Q': {
        'name': '欧奈尔CAN SLIM成长龙头型',
        'regime': '追求高成长、高盈利质量的成长型市场，适合牛市和成长风格占优环境',
        'weights': {
            'E_SUE': 0.15, 'G_NI_YOY': 0.15, 'G_REV_YOY': 0.10,
            'EV_MANAGEMENT_BUY': 0.08, 'EV_SHHOLDER_INCREASE': 0.07,
            'M_NEW_HIGH_20': 0.10, 'M_RS_3M': 0.10, 'M_INDU_MOM': 0.05,
            'I_FUND_HOLD_PCT': 0.08, 'I_FUND_CONSENSUS': 0.04,
            'S_LN_FLOAT': -0.04, 'S_AMT_20_Z': 0.04
        },
        'hard_filter': {
            'E_SUE': {'op': '>', 'value': 0.2},
            'G_NI_YOY': {'op': '>', 'value': 25},
            'I_FUND_HOLD_PCT': {'op': '>', 'value': 5}
        }
    },
    'R': {
        'name': '格雷厄姆深度价值型',
        'regime': '熊市底部、极度悲观市场环境下，寻找低估值高安全边际的价值型标的',
        'weights': {
            'V_PB': -0.20, 'V_PE_TTM': -0.15, 'V_EP': 0.15,
            'L_DEBT_RATIO': -0.10, 'Q_CURRENT_RATIO': 0.08,
            'Q_QUICK_RATIO': 0.07, 'V_DY': 0.10, 'G_ASSET_YOY': 0.05,
            'E_EARNINGS_MOM': 0.05, 'C_TOP10_HOLD': 0.05
        },
        'hard_filter': {
            'V_PB': {'op': '<', 'value': 1.2},
            'V_PE_TTM': {'op': '<', 'value': 15},
            'L_DEBT_RATIO': {'op': '<', 'value': 60},
            'Q_CURRENT_RATIO': {'op': '>', 'value': 1.5}
        }
    },
    'S': {
        'name': '巴菲特GARP型',
        'regime': '追求合理价格成长(GARP)，侧重高质量商业模式和长期竞争优势',
        'weights': {
            'P_ROE': 0.20, 'P_ROA': 0.15, 'P_GPM': 0.10, 'Q_OCF_NI': 0.12,
            'L_DEBT_RATIO': -0.08, 'V_EP': 0.10, 'V_SG': -0.08,
            'I_FUND_HOLD_PCT': 0.07, 'E_EARNINGS_MOM': 0.05,
            'S_LN_FLOAT': -0.05
        },
        'hard_filter': {
            'P_ROE': {'op': '>', 'value': 15},
            'P_ROA': {'op': '>', 'value': 5},
            'P_GPM': {'op': '>', 'value': 30},
            'Q_OCF_NI': {'op': '>', 'value': 1},
            'V_SG': {'op': '<', 'value': 1.5}
        }
    },
    'T': {
        'name': '林奇PEG成长型',
        'regime': '挖掘高成长潜力股，侧重营收增长和合理估值(PEG<1)',
        'weights': {
            'G_REV_YOY': 0.20, 'G_NI_YOY': 0.15, 'V_SG': -0.15,
            'P_ROE': 0.10, 'P_GPM': 0.08, 'Q_ATD': 0.07,
            'M_RS_3M': 0.08, 'I_FUND_NEW': 0.07,
            'S_LN_FLOAT': -0.05, 'S_AMT_20_Z': 0.05
        },
        'hard_filter': {
            'V_SG': {'op': '<', 'value': 1},
            'G_REV_YOY': {'op': '>', 'value': 20},
            'G_NI_YOY': {'op': '>', 'value': 15},
            'P_ROE': {'op': '>', 'value': 10}
        }
    },
    'U': {
        'name': '格林布拉特神奇公式',
        'regime': '价值投资框架，结合高资本回报率和低估值筛选优质标的',
        'weights': {
            'P_ROIC': 0.30, 'V_EV_EBITDA': -0.30, 'V_EP': 0.15,
            'L_DEBT_RATIO': -0.10, 'Q_OCF_NI': 0.08, 'I_FUND_HOLD_PCT': 0.07
        },
        'hard_filter': {
            'P_ROIC': {'op': '>', 'value': 15},
            'V_EV_EBITDA': {'op': '<', 'value': 10},
            'L_DEBT_RATIO': {'op': '<', 'value': 50},
            'Q_OCF_NI': {'op': '>', 'value': 1}
        }
    },
    'V': {
        'name': '索罗斯宏观对冲型',
        'regime': '宏观敏感市场环境下，结合汇率、利率、资金流动等宏观因子的策略',
        'weights': {
            'MF_NORTH_NET_IN_20D': 0.18, 'MF_MARGIN_NET_BUY': 0.15,
            'M_RS_3M': 0.12, 'I_NORTH_HOLD_PCT': 0.10,
            'M_INDU_MOM': 0.10, 'MF_SECTOR_FLOW_IN': 0.10,
            'S_AMT_20_Z': 0.08, 'C_CONCENTRATION': -0.09, 'V_PB': -0.08
        },
        'hard_filter': {
            'MF_NORTH_NET_IN_5D': {'op': '>', 'value': 0},
            'MF_MARGIN_ACCEL': {'op': '>', 'value': 0},
            'M_RS_3M': {'op': '>', 'value': 0}
        }
    },
    'W': {
        'name': '唐奇安趋势跟踪型',
        'regime': '强化趋势跟踪策略，突破交易和均线系统结合',
        'weights': {
            'TP_CHANNEL_BREAK': 0.20, 'M_NEW_HIGH_20': 0.15, 'M_MA_BULL': 0.15,
            'M_MOM_12_1': 0.12, 'M_VOL_ACCEL': 0.10, 'TP_VOL_COMPRESS': 0.08,
            'S_AMT_20_Z': 0.08, 'C_WINNER_RATE': 0.07, 'M_RSI_50_70': 0.05
        },
        'hard_filter': {
            'TP_CHANNEL_BREAK': {'op': '==', 'value': 1},
            'M_MA_BULL': {'op': '==', 'value': 1},
            'M_VOL_ACCEL': {'op': '>', 'value': 1.5}
        }
    },

    # ===== v8.0 高级策略 X-AD =====
    'X': {
        'name': '波动率溢价策略',
        'regime': '利用低波动率异象，选择低波动率高质量股票，在震荡市和熊市末期表现优异',
        'weights': {
            'VOL_20D': -0.25, 'VOL_IDIOSYNCRATIC': -0.20, 'VOL_OMEGA': 0.15,
            'Q_ALT_ZSCORE': 0.15, 'P_ROE': 0.10, 'V_EP': 0.10,
            'S_LN_FLOAT': -0.05
        },
        'hard_filter': {
            'Q_ALT_ZSCORE': {'op': '>', 'value': 2.5}
        }
    },
    'Y': {
        'name': '情绪反转策略',
        'regime': '市场情绪极度悲观时买入，极度乐观时卖出，逆向投资获取超额收益',
        'weights': {
            'SENT_TURNOVER_Z': -0.20, 'SENT_NEW_HIGH_PCT': -0.20,
            'SENT_ADVANCE_DECLINE': -0.15, 'SENT_SHORT_RATIO': 0.15,
            'SENT_COMMENT_SCORE': 0.10, 'M_RSI_OVERSOLD': -0.10,
            'V_BP': 0.10
        },
        'hard_filter': {}
    },
    'Z': {
        'name': '日历效应策略',
        'regime': '利用A股显著的季节性规律，在特定月份配置相应因子获取超额收益',
        'weights': {
            'CAL_SPRING_RALLY': 0.30, 'CAL_SELL_MAY': -0.30,
            'CAL_YEAR_END_SWITCH': 0.30, 'CAL_EARNING_SEASON': 0.10
        },
        'hard_filter': {}
    },
    'AA': {
        'name': '公司治理增强策略',
        'regime': '良好公司治理带来长期超额收益，适合长期投资和价值投资',
        'weights': {
            'GOV_INSIDER_OWN': 0.20, 'GOV_INSIDER_TRADE': 0.20,
            'GOV_DIVIDEND_CONT': 0.15, 'GOV_AUDIT_OPINION': 0.15,
            'GOV_REGULATORY_PENALTY': -0.15, 'GOV_ESG_SCORE': 0.15
        },
        'hard_filter': {
            'GOV_AUDIT_OPINION': {'op': '==', 'value': 1},
            'GOV_REGULATORY_PENALTY': {'op': '==', 'value': 0}
        }
    },
    'AB': {
        'name': '行业轮动增强策略',
        'regime': '行业动量+资金流向+估值三重驱动，捕捉板块轮动机会',
        'weights': {
            'SECTOR_MOM_1M': 0.25, 'SECTOR_RS_3M': 0.20,
            'MF_SECTOR_FLOW_IN': 0.20, 'CONCEPT_HOT_RANK': 0.15,
            'V_EP': 0.10, 'P_ROE': 0.10
        },
        'hard_filter': {
            'SECTOR_MOM_1M': {'op': '>', 'value': 0},
            'MF_SECTOR_FLOW_IN': {'op': '>', 'value': 0}
        }
    },
    'AC': {
        'name': '风险平价多策略',
        'regime': '等风险贡献配置多个策略，降低组合波动，提高夏普比率',
        'weights': {
            'V_EP': 0.20, 'P_ROE': 0.20, 'G_NI_YOY': 0.20,
            'M_MOM_12_1': 0.20, 'S_LN_FLOAT': -0.20
        },
        'hard_filter': {}
    },
    'AD': {
        'name': '因子轮动策略',
        'regime': '根据因子动量动态调整因子权重，自适应市场风格变化',
        'weights': {
            'V_EP': 0.20, 'P_ROE': 0.20, 'G_NI_YOY': 0.20,
            'M_MOM_12_1': 0.20, 'S_LN_FLOAT': -0.20
        },
        'hard_filter': {}
    },
    
    # ===== 龙头选股策略 =====
    'DRAGON': {
        'name': '龙头趋势共振型',
        'regime': '牛市主升浪、强势震荡市，追求高确定性趋势龙头股',
        'weights': {
            'D_F_SCORE': 0.15,
            'D_RS_RATING': 0.20,
            'D_HURST_EXP': 0.15,
            'D_Z_SCORE_60': 0.10,
            'D_OBV_TREND': 0.10,
            'D_MACD_DIF': 0.10,
            'D_MACD_HIST_DELTA': 0.10,
            'D_VWAP_BIAS': 0.05,
            'D_EARNINGS_QUALITY': 0.05,
        },
        'hard_filter': {
            'D_F_SCORE': {'op': '>=', 'value': 7},
            'D_RS_RATING': {'op': '>', 'value': 80},
            'D_HURST_EXP': {'op': '>', 'value': 0.6},
            'D_Z_SCORE_60': {'op': 'between', 'value': [0.5, 2.0]},
            'D_TURNRATE': {'op': 'between', 'value': [2.0, 12.0]},
            'D_CIRC_MCAP': {'op': 'between', 'value': [50.0, 300.0]},
        },
    },
    
    # ===== V-Hunter T+0 高弹性策略 =====
    'T0_REBOUND': {
        'name': 'V-Hunter T+0高弹性',
        'regime': '超跌反弹、日内T+0操作，适合高波动市场环境',
        'weights': {
            # 核心信号（与硬过滤对应）
            'T0_MA60_BIAS': 0.22,
            'T0_MACD_UNDERWATER': 0.17,
            'T0_TURNOVER_RANK': 0.17,
            'T0_OCF_NI': 0.05,
            'T0_PROFITABLE': 0.05,
            # stk_factor_pro 扩展辅助打分
            'T0_KDJ_OVERSOLD': 0.06,
            'T0_RSI6_OVERSOLD': 0.06,
            'T0_WR_OVERSOLD': 0.04,
            'T0_BOLL_BREAK_LOWER': 0.04,
            'T0_BIAS_EXTREME': 0.06,
            'T0_DOWN_DAYS': 0.05,
            'T0_VOLUME_RATIO': 0.04,
            'T0_ATR_ELASTICITY': 0.04,
        },
        'hard_filter': {
            'T0_MA60_BIAS': {'op': '<', 'value': -0.15},
            'T0_MACD_UNDERWATER': {'op': '>', 'value': 0},
            'T0_TURNOVER_RANK': {'op': '>', 'value': 0.7},
            'T0_IS_ST': {'op': '==', 'value': 0},
            # circ_mv 单位为万元（Tushare daily_basic 规范）；30e4万元=30亿元，200e4万元=200亿元
            'circ_mv': {'op': 'between', 'value': [30e4, 200e4]},
        }
    },
}


# ============================================================
# 策略版本信息
# ============================================================
STRATEGY_VERSIONS = {
    'v6.0': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'],
    'v7.0': ['I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'],
    'v7.1': ['Q', 'R', 'S', 'T', 'U', 'V', 'W'],
    'v8.0': ['X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD'],
    'v9.0': ['DRAGON', 'T0_REBOUND'],  # 龙头策略 + T+0反弹策略
}

# 风险平价多策略(AC)的子策略权重配置
RISK_PARITY_CONFIG = {
    'A': {'risk_weight': 0.20, 'expected_sharpe': (0.8, 1.2), 'regime': '熊市/震荡市'},
    'B': {'risk_weight': 0.15, 'expected_sharpe': (1.0, 1.5), 'regime': '牛市'},
    'C': {'risk_weight': 0.20, 'expected_sharpe': (0.9, 1.3), 'regime': '全市场'},
    'X': {'risk_weight': 0.15, 'expected_sharpe': (1.1, 1.6), 'regime': '震荡市/熊市末期'},
    'M': {'risk_weight': 0.15, 'expected_sharpe': (1.2, 1.8), 'regime': '事件驱动期'},
    'AB': {'risk_weight': 0.15, 'expected_sharpe': (1.0, 1.4), 'regime': '板块轮动期'},
}


# ============================================================
# 导出函数
# ============================================================

def get_strategy_combo(strategy_id: str) -> dict:
    """
    获取指定策略的因子权重配置

    Args:
        strategy_id: 策略ID，如 'A', 'B', ..., 'AD'

    Returns:
        dict: 策略权重字典 {factor_id: weight}，负权重表示负向因子
              如果策略ID不存在，返回均衡综合型(D)的权重
    """
    strategy_id = strategy_id.upper()
    combo = STRATEGY_COMBOS.get(strategy_id)
    if combo is None:
        # 默认返回均衡综合型(D)
        return STRATEGY_COMBOS['D']['weights'].copy()
    return combo['weights'].copy()


def list_all_strategies() -> list:
    """
    列出所有30套策略的基本信息

    Returns:
        list[dict]: 每个策略包含 id, name, regime, factor_count, has_hard_filter
    """
    result = []
    for sid, combo in STRATEGY_COMBOS.items():
        result.append({
            'id': sid,
            'name': combo['name'],
            'regime': combo['regime'],
            'factor_count': len(combo['weights']),
            'has_hard_filter': bool(combo['hard_filter']),
        })
    return result


def get_strategy_info(strategy_id: str) -> dict:
    """
    获取指定策略的完整信息

    Args:
        strategy_id: 策略ID，如 'A', 'B', ..., 'AD'

    Returns:
        dict: 策略完整信息，包含 name, regime, weights, hard_filter
              如果策略ID不存在，返回None
    """
    strategy_id = strategy_id.upper()
    combo = STRATEGY_COMBOS.get(strategy_id)
    if combo is None:
        return None
    return {
        'id': strategy_id,
        'name': combo['name'],
        'regime': combo['regime'],
        'weights': combo['weights'].copy(),
        'hard_filter': combo['hard_filter'].copy(),
    }


def get_strategies_by_version(version: str) -> list:
    """
    获取指定版本的策略ID列表

    Args:
        version: 版本号，如 'v6.0', 'v7.0', 'v7.1', 'v8.0'

    Returns:
        list[str]: 该版本的策略ID列表
    """
    return STRATEGY_VERSIONS.get(version, [])


def validate_strategy_weights(strategy_id: str) -> dict:
    """
    验证策略权重是否合法（权重绝对值之和应接近1.0）

    Args:
        strategy_id: 策略ID

    Returns:
        dict: 包含 is_valid, total_abs_weight, deviation 信息
    """
    weights = get_strategy_combo(strategy_id)
    total_abs = sum(abs(w) for w in weights.values())
    deviation = abs(total_abs - 1.0)
    return {
        'strategy_id': strategy_id,
        'is_valid': deviation < 0.05,  # 允许5%偏差
        'total_abs_weight': round(total_abs, 4),
        'deviation': round(deviation, 4),
    }


if __name__ == '__main__':
    # 测试代码
    print("=" * 60)
    print("30套策略权重配置验证")
    print("=" * 60)

    # 列出所有策略
    all_strategies = list_all_strategies()
    print(f"\n总策略数: {len(all_strategies)}")
    for s in all_strategies:
        filter_mark = " [过滤]" if s['has_hard_filter'] else ""
        print(f"  {s['id']:>3s}: {s['name']:<20s} | 因子数: {s['factor_count']:>2d}{filter_mark}")

    # 验证权重
    print("\n权重验证:")
    invalid_count = 0
    for sid in STRATEGY_COMBOS:
        result = validate_strategy_weights(sid)
        if not result['is_valid']:
            invalid_count += 1
            print(f"  {sid}: 总权重={result['total_abs_weight']:.4f} 偏差={result['deviation']:.4f} ⚠️")
    if invalid_count == 0:
        print("  所有策略权重验证通过 ✓")

    # 测试获取策略
    print("\n策略A详情:")
    info = get_strategy_info('A')
    if info:
        print(f"  名称: {info['name']}")
        print(f"  环境: {info['regime']}")
        print(f"  权重: {info['weights']}")

    # 测试默认策略
    print("\n不存在的策略(返回默认D):")
    default_weights = get_strategy_combo('ZZ')
    print(f"  {default_weights}")
