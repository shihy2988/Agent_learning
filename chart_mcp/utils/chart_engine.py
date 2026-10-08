# -*- coding: utf-8 -*-
"""
============================================================
文件名: chart_engine.py
功能描述: 通用图表生成模块（独立 MCP 服务版本，不依赖任何业务代码）
============================================================

提供工业数据的可视化图表生成能力。
支持两大引擎：

【matplotlib 引擎】-- 默认引擎，输出 PNG 静态图片
  适合：MCP 工具返回、嵌入 HTML、打印报告、大模型分析

【Plotly 引擎】 -- 可选引擎，输出 HTML 交互式图表
  适合：Web 前端展示、数据探索、悬停查看详情

支持的图表类型（11种）：
 1. trend       - 时序趋势折线图
 2. bar         - 多指标分组柱状图
 3. pie         - 饼图/环形图
 4. scatter     - 散点图
 5. heatmap     - 热力图
 6. boxplot     - 箱线图
 7. area        - 面积图
 8. gauge       - 仪表盘
 9. dual_axis   - 双Y轴图
10. radar       - 雷达图
11. energy      - 能耗统计图

依赖:
  - matplotlib（必须）
  - numpy（必须）
  - plotly（可选，用于交互式图表）
"""

import io
import os
import base64
import logging
import time
from typing import List, Dict, Optional, Any

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np

logger = logging.getLogger(__name__)

# ============================================================
# Plotly 可选导入
# ============================================================
_PLOTLY_AVAILABLE = False
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    _PLOTLY_AVAILABLE = True
    logger.info("Plotly 导入成功，交互式图表功能可用")
except ImportError:
    logger.info("Plotly 未安装，仅支持 matplotlib 静态图表。安装: pip install plotly")

# ============================================================
# 中文字体配置
# ============================================================
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os


def _setup_chinese_font():
    chinese_fonts = [
        'Microsoft YaHei',
        'SimHei',
        'WenQuanYi Micro Hei',
        'WenQuanYi Zen Hei',
        'Noto Sans CJK SC',
        'AR PL UMing CN',
        'DejaVu Sans',
    ]

    # 1. 优先检查本地字体文件
    local_font = "/home/code/chart_mcp/utils/SIMHEI.TTF"

    if os.path.exists(local_font):
        try:
            fm.fontManager.addfont(local_font)

            font_prop = fm.FontProperties(fname=local_font)
            font_name = font_prop.get_name()

            matplotlib.rcParams['font.sans-serif'] = [font_name]
            matplotlib.rcParams['axes.unicode_minus'] = False

            logger.info(f"加载本地中文字体成功: {font_name}")
            return font_name

        except Exception as e:
            logger.warning(f"本地字体加载失败: {e}")


    # 2. 查找系统字体
    available_fonts = {
        f.name for f in fm.fontManager.ttflist
    }

    for font_name in chinese_fonts:
        if font_name in available_fonts:
            matplotlib.rcParams['font.sans-serif'] = [font_name]
            matplotlib.rcParams['axes.unicode_minus'] = False

            logger.info(f"加载系统中文字体成功: {font_name}")
            return font_name


    # 3. fallback
    logger.warning(
        "无法配置中文字体，图表中文可能显示为方块"
    )

    matplotlib.rcParams['font.sans-serif'] = ['DejaVu Sans']
    matplotlib.rcParams['axes.unicode_minus'] = False

    return 'DejaVu Sans' 

_CHINESE_FONT_READY = False

def _ensure_font():
    global _CHINESE_FONT_READY
    if not _CHINESE_FONT_READY:
        _setup_chinese_font()
        _CHINESE_FONT_READY = True

# ============================================================
# 通用常量与工具函数
# ============================================================
CHART_OUTPUT_DIR = "/home/code/imgs"
os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)

COLORS_10 = ['#2196F3','#FF5722','#4CAF50','#FF9800','#9C27B0','#00BCD4','#795548','#607D8B','#E91E63','#3F51B5']
COLORS_20 = COLORS_10 + ['#009688','#CDDC39','#FFC107','#8BC34A','#03A9F4','#FF5252','#536DFE','#FF4081','#64FFDA','#FFD740']

CHART_TYPE_MAP = {
    "trend":"时序趋势折线图","bar":"多指标分组柱状图","pie":"饼图/环形图",
    "scatter":"散点图","heatmap":"热力图","boxplot":"箱线图",
    "area":"面积图","gauge":"仪表盘","dual_axis":"双Y轴图",
    "radar":"雷达图","energy":"能耗统计图",
}
SUPPORTED_CHART_TYPES = list(CHART_TYPE_MAP.keys())

def _cleanup_old_charts(max_files: int = 100):
    try:
        all_files = []
        for ext in ['.png', '.html']:
            all_files.extend([os.path.join(CHART_OUTPUT_DIR, f)
                             for f in os.listdir(CHART_OUTPUT_DIR) if f.endswith(ext)])
        if len(all_files) > max_files:
            all_files.sort(key=lambda x: os.path.getmtime(x))
            for old_file in all_files[:-max_files]:
                try: os.remove(old_file)
                except OSError: pass
    except Exception:
        pass

def _fig_to_base64(fig, dpi=80):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=dpi, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def _fig_to_file(fig, dpi=80):
    date_str = time.strftime('%Y%m%d')
    os.makedirs(os.path.join(CHART_OUTPUT_DIR, date_str), exist_ok=True)
    filename = f"{date_str}/chart_{time.strftime('%H%M%S')}_{os.urandom(2).hex()}.png"
    filepath = os.path.join(CHART_OUTPUT_DIR, filename)
    filepath1 = os.path.join("http://10.11.3.210:4567", filename)
    fig.savefig(filepath, format='png', dpi=dpi, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close(fig)
    return filepath, filepath1

def _ensure_float(value):
    if value is None: return None
    try: return float(value)
    except (ValueError, TypeError): return None

def _get_colors(n, color_scheme=None):
    if color_scheme:
        return color_scheme[:n] if len(color_scheme) >= n else (color_scheme * (n // len(color_scheme) + 1))[:n]
    if n <= len(COLORS_20): return COLORS_20[:n]
    return COLORS_20 * (n // len(COLORS_20) + 1)

def _format_value(val, decimals=2):
    if val is None: return "N/A"
    fv = float(val)
    if fv == int(fv): return str(int(fv))
    return f"{fv:.{decimals}f}"

def _apply_clean_style(ax):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_alpha(0.3)
    ax.spines['bottom'].set_alpha(0.3)
    ax.grid(True, linestyle='--', alpha=0.3, color='gray')

# ============================================================
# 图表类型1: 时序趋势折线图 (trend)
# ============================================================
def generate_trend_chart(title, x_labels, series_list, y_label="", figsize=(12,6), dpi=120):
    _ensure_font()
    if not series_list or not x_labels:
        raise ValueError("x_labels 和 series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    x_pos = range(len(x_labels))
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        line_style = series.get("line_style", "-")
        marker = series.get("marker", "o")
        line_width = series.get("line_width", 2)
        marker_size = series.get("marker_size", 5)
        clean_data = [_ensure_float(v) for v in data]
        clean_x = [j for j, v in enumerate(clean_data) if v is not None]
        clean_y = [clean_data[j] for j in clean_x]
        if not clean_y: continue
        label = f"{name} ({unit})" if unit else name
        ax.plot(clean_x, clean_y, linestyle=line_style, marker=marker, color=color,
                linewidth=line_width, markersize=marker_size, label=label, alpha=0.9)
        if len(clean_x) <= 20:
            for xj, yj in zip(clean_x, clean_y):
                if yj is not None:
                    ax.annotate(_format_value(yj, 1), (xj, yj), textcoords="offset points",
                                xytext=(0, 8), ha='center', fontsize=7, color=color, alpha=0.8)
    ax.set_xticks(list(x_pos))
    if len(x_labels) > 20:
        step = max(1, len(x_labels) // 15)
        ax.set_xticks(x_pos[::step])
        ax.set_xticklabels([x_labels[i] for i in x_pos[::step]], rotation=90, ha='center', fontsize=8)
    else:
        ax.set_xticklabels(x_labels, rotation=90, ha='center', fontsize=9)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    if y_label: ax.set_ylabel(y_label, fontsize=12)
    ax.set_xlabel('时间', fontsize=11)
    _apply_clean_style(ax)
    if any(s.get("name") for s in series_list):
        ax.legend(loc='upper left', fontsize=9, framealpha=0.8, ncol=min(3, len(series_list)))
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型2: 多指标分组柱状图 (bar)
# ============================================================
def generate_bar_chart(title, categories, series_list, y_label="", figsize=(10,6), dpi=120, horizontal=False):
    _ensure_font()
    if not series_list or not categories:
        raise ValueError("categories 和 series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    n_categories = len(categories)
    n_series = len(series_list)
    bar_width = 0.8 / n_series
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        clean_data = [_ensure_float(v) or 0 for v in data]
        if horizontal:
            positions = np.arange(n_categories) + i * bar_width
            bars = ax.barh(positions, clean_data, bar_width,
                           label=f"{name} ({unit})" if unit else name, color=color, alpha=0.85)
            for bar_item, val in zip(bars, clean_data):
                ax.text(bar_item.get_width() + max(clean_data)*0.01 if clean_data else 0,
                        bar_item.get_y()+bar_item.get_height()/2,
                        _format_value(val,1), va='center', fontsize=8, color=color)
        else:
            positions = np.arange(n_categories) + i * bar_width
            bars = ax.bar(positions, clean_data, bar_width,
                          label=f"{name} ({unit})" if unit else name, color=color, alpha=0.85)
            max_val = max(clean_data) if clean_data else 0
            for bar_item, val in zip(bars, clean_data):
                ax.text(bar_item.get_x()+bar_item.get_width()/2, bar_item.get_height()+max_val*0.01,
                        _format_value(val,1), ha='center', fontsize=8, color=color)
    if horizontal:
        ax.set_yticks(np.arange(n_categories)+(n_series-1)*bar_width/2)
        ax.set_yticklabels(categories, fontsize=10)
        if y_label: ax.set_xlabel(y_label, fontsize=12)
    else:
        ax.set_xticks(np.arange(n_categories)+(n_series-1)*bar_width/2)
        ax.set_xticklabels(categories, rotation=45, ha='right', fontsize=10)
        if y_label: ax.set_ylabel(y_label, fontsize=12)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    _apply_clean_style(ax)
    ax.legend(loc='upper right', fontsize=9, framealpha=0.8)
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型3: 饼图/环形图 (pie)
# ============================================================
def generate_pie_chart(title, labels, values, colors=None, figsize=(8,6), dpi=120,
                       show_percentage=True, donut=False):
    _ensure_font()
    if not labels or not values: raise ValueError("labels 和 values 不能为空")
    if len(labels) != len(values): raise ValueError("labels 和 values 长度必须一致")
    if colors is None: colors = _get_colors(len(labels))
    fig, ax = plt.subplots(figsize=figsize)
    if show_percentage:
        total = sum(values)
        def make_autopct(vals):
            def autopct(pct):
                val = int(round(pct*total/100.0))
                return f'{pct:.1f}%\n({val})'
            return autopct
        autopct = make_autopct(values)
    else:
        autopct = None
    wedgeprops = {'edgecolor': 'white', 'linewidth': 1.5}
    if donut: wedgeprops['width'] = 0.4
    wedges, texts, autotexts = ax.pie(
        values, labels=labels, autopct=autopct, colors=colors,
        startangle=90, pctdistance=0.75, labeldistance=1.1,
        wedgeprops=wedgeprops, textprops={'fontsize': 10})
    if autotexts:
        for at in autotexts: at.set_fontsize(9); at.set_fontweight('bold')
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    legend_labels = [f'{l} ({v})' for l, v in zip(labels, values)]
    ax.legend(wedges, legend_labels, title="图例", loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1), fontsize=9, framealpha=0.8)
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型4: 散点图 (scatter)
# ============================================================
def generate_scatter_chart(title, series_list, x_label="", y_label="",
                           figsize=(10,6), dpi=120, show_trendline=True):
    _ensure_font()
    if not series_list: raise ValueError("series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        x_data = [_ensure_float(v) for v in series.get("x", [])]
        y_data = [_ensure_float(v) for v in series.get("y", [])]
        color = series.get("color", colors[i])
        marker_size = series.get("marker_size", 60)
        clean_points = [(x, y) for x, y in zip(x_data, y_data) if x is not None and y is not None]
        if not clean_points: continue
        cx, cy = zip(*clean_points)
        ax.scatter(cx, cy, c=color, label=name, s=marker_size, alpha=0.7, edgecolors='white', linewidth=0.5)
        if show_trendline and len(clean_points) >= 3:
            z = np.polyfit(cx, cy, 1)
            p = np.poly1d(z)
            x_sorted = sorted(cx)
            ax.plot(x_sorted, p(x_sorted), '--', color=color, linewidth=1.5, alpha=0.6)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    if x_label: ax.set_xlabel(x_label, fontsize=12)
    if y_label: ax.set_ylabel(y_label, fontsize=12)
    _apply_clean_style(ax)
    ax.legend(loc='upper right', fontsize=9, framealpha=0.8)
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型5: 热力图 (heatmap)
# ============================================================
def generate_heatmap_chart(title, row_labels, col_labels, data_matrix,
                           x_label="", y_label="", figsize=(12,8), dpi=120,
                           color_scheme="coolwarm", annotate=True):
    _ensure_font()
    if not data_matrix or not row_labels or not col_labels:
        raise ValueError("row_labels, col_labels 和 data_matrix 不能为空")
    data_array = np.array(data_matrix, dtype=float)
    fig, ax = plt.subplots(figsize=figsize)
    im = ax.imshow(data_array, cmap=color_scheme, aspect='auto')
    ax.set_xticks(range(len(col_labels))); ax.set_yticks(range(len(row_labels)))
    ax.set_xticklabels(col_labels, rotation=90, ha='center', fontsize=8)
    ax.set_yticklabels(row_labels, fontsize=9)
    if annotate and data_array.size <= 200:
        for i in range(len(row_labels)):
            for j in range(len(col_labels)):
                val = data_array[i, j]
                if not np.isnan(val):
                    tc = 'white' if abs(val) > (np.nanmax(data_array)-np.nanmin(data_array))/2 else 'black'
                    ax.text(j, i, _format_value(val,1), ha='center', va='center', fontsize=7, color=tc)
    cbar = plt.colorbar(im, ax=ax, shrink=0.85, pad=0.02)
    cbar.ax.tick_params(labelsize=8)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    if x_label: ax.set_xlabel(x_label, fontsize=12)
    if y_label: ax.set_ylabel(y_label, fontsize=12)
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型6: 箱线图 (boxplot)
# ============================================================
def generate_boxplot_chart(title, series_list, y_label="", figsize=(10,6), dpi=120, horizontal=False):
    _ensure_font()
    if not series_list: raise ValueError("series_list 不能为空")
    colors = _get_colors(len(series_list))
    labels = [s.get("name", f"系列{i+1}") for i, s in enumerate(series_list)]
    data_groups = [[_ensure_float(v) or np.nan for v in s.get("data", [])] for s in series_list]
    box_colors = [s.get("color", colors[i]) for i, s in enumerate(series_list)]
    fig, ax = plt.subplots(figsize=figsize)
    if horizontal:
        bp = ax.boxplot(data_groups, tick_labels=labels, vert=False, patch_artist=True,
                         showmeans=True, meanprops=dict(marker='D', markerfacecolor='red', markersize=6))
        if y_label: ax.set_xlabel(y_label, fontsize=12)
    else:
        bp = ax.boxplot(data_groups, tick_labels=labels, vert=True, patch_artist=True,
                         showmeans=True, meanprops=dict(marker='D', markerfacecolor='red', markersize=6))
        if y_label: ax.set_ylabel(y_label, fontsize=12)
    for patch, color in zip(bp['boxes'], box_colors):
        patch.set_facecolor(color); patch.set_alpha(0.6)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    _apply_clean_style(ax)
    from matplotlib.lines import Line2D
    ax.legend(handles=[Line2D([0],[0], marker='D', color='w', markerfacecolor='red',
                               markersize=8, label='均值')], loc='upper right', fontsize=9)
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型7: 面积图 (area)
# ============================================================
def generate_area_chart(title, x_labels, series_list, y_label="", figsize=(12,6), dpi=120,
                        stacked=True, alpha=0.6):
    _ensure_font()
    if not series_list or not x_labels:
        raise ValueError("x_labels 和 series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    x_pos = range(len(x_labels))
    all_data = []
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        clean_data = np.array([_ensure_float(v) or 0 for v in data])
        all_data.append({"name":name, "data":clean_data, "unit":unit, "color":color})
    if stacked:
        y_stack = np.zeros(len(x_labels))
        for item in all_data:
            ax.fill_between(x_pos, y_stack, y_stack+item["data"],
                            alpha=alpha, color=item["color"],
                            label=f'{item["name"]} ({item["unit"]})' if item["unit"] else item["name"])
            ax.plot(x_pos, y_stack+item["data"], color=item["color"], linewidth=1.5, alpha=0.9)
            y_stack = y_stack + item["data"]
    else:
        for item in all_data:
            ax.fill_between(x_pos, item["data"], alpha=alpha, color=item["color"],
                            label=f'{item["name"]} ({item["unit"]})' if item["unit"] else item["name"])
            ax.plot(x_pos, item["data"], color=item["color"], linewidth=1.5, alpha=0.9)
    ax.set_xticks(list(x_pos))
    if len(x_labels) > 20:
        step = max(1, len(x_labels) // 15)
        ax.set_xticks(x_pos[::step])
        ax.set_xticklabels([x_labels[i] for i in x_pos[::step]], rotation=90, ha='center', fontsize=8)
    else:
        ax.set_xticklabels(x_labels, rotation=90, ha='center', fontsize=9)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    if y_label: ax.set_ylabel(y_label, fontsize=12)
    _apply_clean_style(ax)
    ax.legend(loc='upper left', fontsize=9, framealpha=0.8, ncol=min(3, len(series_list)))
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型8: 仪表盘 (gauge)
# ============================================================
def generate_gauge_chart(title, value, min_val=0, max_val=100, unit="",
                         thresholds=None, figsize=(8,6), dpi=120):
    _ensure_font()
    if thresholds is None:
        thresholds = [
            {"label":"正常","range":[0,60],"color":"#4CAF50"},
            {"label":"注意","range":[60,85],"color":"#FF9800"},
            {"label":"报警","range":[85,100],"color":"#FF5722"},
        ]
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': 'polar'})
    ax.set_theta_zero_location('N'); ax.set_theta_direction(-1)
    ax.set_thetamin(0); ax.set_thetamax(180)
    total_range = max_val - min_val
    for th in thresholds:
        rng = th["range"]
        start_angle = (rng[0]-min_val)/total_range*180
        end_angle = (rng[1]-min_val)/total_range*180
        theta = np.linspace(start_angle, end_angle, 50)
        theta_rad = np.radians(theta)
        ax.fill_between(theta_rad, np.full_like(theta_rad,1.0), np.full_like(theta_rad,1.4),
                        color=th["color"], alpha=0.6)
        mid_theta = np.radians((start_angle+end_angle)/2)
        ax.text(mid_theta, 1.55, th.get("label",""), ha='center', va='center',
                fontsize=8, fontweight='bold', color=th["color"])
    pointer_angle = (value-min_val)/total_range*180
    pointer_rad = np.radians(pointer_angle)
    ax.annotate('', xy=(pointer_rad, 1.3), xytext=(pointer_rad, 0.2),
                arrowprops=dict(arrowstyle='->', color='#333', lw=3))
    ax.scatter(pointer_rad, 0.2, s=150, color='#333', zorder=10)
    tick_angles = np.linspace(0, 180, 9); tick_rad = np.radians(tick_angles)
    tick_values = np.linspace(min_val, max_val, 9)
    for ta, tv in zip(tick_rad, tick_values):
        ax.plot([ta, ta], [0.9, 1.0], color='gray', linewidth=1)
        ax.text(ta, 0.75, f'{tv:.0f}', ha='center', va='center', fontsize=7, color='gray')
    display_text = f'{_format_value(value,1)} {unit}' if unit else f'{_format_value(value,1)}'
    ax.text(np.radians(90), 0.15, display_text, ha='center', va='center',
            fontsize=28, fontweight='bold', color='#333')
    ax.text(np.radians(90), -0.05, title, ha='center', va='center', fontsize=12, color='#666')
    ax.set_ylim(0, 1.7); ax.set_xticklabels([]); ax.set_yticklabels([])
    ax.grid(False); ax.spines.clear()
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型9: 双Y轴图 (dual_axis)
# ============================================================
def generate_dual_axis_chart(title, x_labels, left_series_list, right_series_list,
                             left_y_label="", right_y_label="", figsize=(12,6), dpi=120):
    _ensure_font()
    if not x_labels or (not left_series_list and not right_series_list):
        raise ValueError("x_labels 和至少一个 series_list 不能为空")
    left_colors = _get_colors(len(left_series_list)) if left_series_list else []
    right_colors = ['#FF5722','#9C27B0','#E91E63','#795548','#607D8B'][:len(right_series_list)] if right_series_list else []
    fig, ax1 = plt.subplots(figsize=figsize)
    x_pos = range(len(x_labels))
    lines_left = []
    for i, series in enumerate(left_series_list):
        name = series.get("name", f"左系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", left_colors[i] if left_colors else '#2196F3')
        clean_data = [_ensure_float(v) for v in data]
        clean_x = [j for j, v in enumerate(clean_data) if v is not None]
        clean_y = [clean_data[j] for j in clean_x]
        if not clean_y: continue
        label = f"{name} ({unit})" if unit else name
        line, = ax1.plot(clean_x, clean_y, marker='o', color=color, linewidth=2, markersize=5, label=label, alpha=0.9)
        lines_left.append(line)
    ax1.set_xlabel('时间', fontsize=11)
    if left_y_label: ax1.set_ylabel(left_y_label, fontsize=12, color='#333')
    ax2 = ax1.twinx()
    lines_right = []
    for i, series in enumerate(right_series_list):
        name = series.get("name", f"右系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", right_colors[i] if right_colors else '#FF5722')
        clean_data = [_ensure_float(v) for v in data]
        clean_x = [j for j, v in enumerate(clean_data) if v is not None]
        clean_y = [clean_data[j] for j in clean_x]
        if not clean_y: continue
        label = f"{name} ({unit})" if unit else name
        line, = ax2.plot(clean_x, clean_y, marker='s', color=color, linewidth=2, markersize=5,
                         label=label, alpha=0.9, linestyle='--')
        lines_right.append(line)
    if right_y_label: ax2.set_ylabel(right_y_label, fontsize=12, color='#FF5722')
    ax1.set_xticks(list(x_pos))
    if len(x_labels) > 20:
        step = max(1, len(x_labels)//15)
        ax1.set_xticks(x_pos[::step])
        ax1.set_xticklabels([x_labels[i] for i in x_pos[::step]], rotation=90, ha='center', fontsize=8)
    else:
        ax1.set_xticklabels(x_labels, rotation=90, ha='center', fontsize=9)
    ax1.set_title(title, fontsize=16, fontweight='bold', pad=15)
    all_lines = lines_left + lines_right
    if all_lines:
        ax1.legend(all_lines, [l.get_label() for l in all_lines],
                   loc='upper left', fontsize=9, framealpha=0.8, ncol=min(3, len(all_lines)))
    ax1.spines['top'].set_visible(False)
    ax1.grid(True, linestyle='--', alpha=0.3, color='gray')
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型10: 雷达图 (radar)
# ============================================================
def generate_radar_chart(title, categories, series_list, figsize=(8,8), dpi=120, fill=True):
    _ensure_font()
    if not categories or not series_list:
        raise ValueError("categories 和 series_list 不能为空")
    n = len(categories)
    angles = np.linspace(0, 2*np.pi, n, endpoint=False).tolist()
    angles += angles[:1]
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': 'polar'})
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        if len(data) != n:
            raise ValueError(f"系列 '{name}' 的 data 长度 ({len(data)}) 与 categories ({n}) 不一致")
        color = series.get("color", colors[i])
        clean_data = [_ensure_float(v) or 0 for v in data]
        clean_data += clean_data[:1]
        ax.plot(angles, clean_data, 'o-', color=color, linewidth=2, markersize=6, label=name)
        if fill: ax.fill(angles, clean_data, alpha=0.15, color=color)
    ax.set_xticks(angles[:-1]); ax.set_xticklabels(categories, fontsize=11)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=25)
    ax.legend(loc='upper right', bbox_to_anchor=(1.2, 1.1), fontsize=9, framealpha=0.8)
    ax.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 图表类型11: 能耗统计图 (energy)
# ============================================================
def generate_energy_chart(title, daily_labels, daily_values,
                          hourly_labels=None, hourly_values=None,
                          unit="kWh", figsize=(14,6), dpi=120):
    _ensure_font()
    fig, ax1 = plt.subplots(1, 1, figsize=figsize)
    clean_daily = [_ensure_float(v) or 0 for v in daily_values]
    bars = ax1.bar(range(len(daily_labels)), clean_daily,
                   color='#2196F3', alpha=0.8, edgecolor='white', linewidth=0.5)
    for bar, val in zip(bars, clean_daily):
        ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+max(clean_daily)*0.02,
                 _format_value(val,1), ha='center', fontsize=8, color='#333')
    if len(clean_daily) > 1:
        z = np.polyfit(range(len(clean_daily)), clean_daily, 1)
        p = np.poly1d(z)
        ax1.plot(range(len(clean_daily)), p(range(len(clean_daily))),
                 '--', color='#FF5722', linewidth=2, alpha=0.7, label='趋势线')
    ax1.set_xticks(range(len(daily_labels)))
    ax1.set_xticklabels(daily_labels, rotation=90, ha='center', fontsize=9)
    ax1.set_ylabel(f'能耗 ({unit})', fontsize=12)
    ax1.set_title(title, fontsize=14, fontweight='bold')
    _apply_clean_style(ax1)
    if len(clean_daily) > 1: ax1.legend(fontsize=9)
    plt.tight_layout()
    return _fig_to_base64(fig, dpi)

# ============================================================
# 内部构建函数（供 generate_chart_with_file 使用）
# ============================================================
def _build_trend_fig(title, x_labels, series_list, y_label, figsize):
    if not series_list or not x_labels: raise ValueError("x_labels 和 series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    x_pos = range(len(x_labels))
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        line_style = series.get("line_style", "-")
        marker = series.get("marker", "o")
        line_width = series.get("line_width", 2)
        marker_size = series.get("marker_size", 5)
        clean_data = [_ensure_float(v) for v in data]
        clean_x = [j for j, v in enumerate(clean_data) if v is not None]
        clean_y = [clean_data[j] for j in clean_x]
        if not clean_y: continue
        label = f"{name} ({unit})" if unit else name
        ax.plot(clean_x, clean_y, linestyle=line_style, marker=marker, color=color,
                linewidth=line_width, markersize=marker_size, label=label, alpha=0.9)
        if len(clean_x) <= 15:
            for xj, yj in zip(clean_x, clean_y):
                if yj is not None:
                    ax.annotate(_format_value(yj,1), (xj, yj), textcoords="offset points",
                                xytext=(0,8), ha='center', fontsize=7, color=color, alpha=0.8)
    ax.set_xticks(list(x_pos))
    if len(x_labels) > 20:
        step = max(1, len(x_labels)//15)
        ax.set_xticks(x_pos[::step])
        ax.set_xticklabels([x_labels[i] for i in x_pos[::step]], rotation=90, ha='center', fontsize=8)
    else:
        ax.set_xticklabels(x_labels, rotation=90, ha='center', fontsize=9)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    if y_label: ax.set_ylabel(y_label, fontsize=11)
    ax.set_xlabel('时间', fontsize=10)
    _apply_clean_style(ax)
    if any(s.get("name") for s in series_list):
        ax.legend(loc='upper left', fontsize=8, framealpha=0.8, ncol=min(3, len(series_list)))
    plt.tight_layout()
    return fig

def _build_bar_fig(title, categories, series_list, y_label, horizontal, figsize):
    if not series_list or not categories: raise ValueError("categories 和 series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    n_categories = len(categories); n_series = len(series_list)
    bar_width = 0.8 / n_series
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        clean_data = [_ensure_float(v) or 0 for v in data]
        if horizontal:
            positions = np.arange(n_categories) + i * bar_width
            bars = ax.barh(positions, clean_data, bar_width,
                           label=f"{name} ({unit})" if unit else name, color=color, alpha=0.85)
            for bi, val in zip(bars, clean_data):
                ax.text(bi.get_width()+max(clean_data)*0.01 if clean_data else 0,
                        bi.get_y()+bi.get_height()/2, _format_value(val,1), va='center', fontsize=8, color=color)
        else:
            positions = np.arange(n_categories) + i * bar_width
            bars = ax.bar(positions, clean_data, bar_width,
                          label=f"{name} ({unit})" if unit else name, color=color, alpha=0.85)
            max_val = max(clean_data) if clean_data else 0
            for bi, val in zip(bars, clean_data):
                ax.text(bi.get_x()+bi.get_width()/2, bi.get_height()+max_val*0.01,
                        _format_value(val,1), ha='center', fontsize=8, color=color)
    if horizontal:
        ax.set_yticks(np.arange(n_categories)+(n_series-1)*bar_width/2)
        ax.set_yticklabels(categories, fontsize=10)
        if y_label: ax.set_xlabel(y_label, fontsize=11)
    else:
        ax.set_xticks(np.arange(n_categories)+(n_series-1)*bar_width/2)
        ax.set_xticklabels(categories, rotation=45, ha='right', fontsize=10)
        if y_label: ax.set_ylabel(y_label, fontsize=11)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    _apply_clean_style(ax)
    ax.legend(loc='upper right', fontsize=8, framealpha=0.8)
    plt.tight_layout()
    return fig

def _build_pie_fig(title, labels, values, colors, figsize):
    if not labels or not values: raise ValueError("labels 和 values 不能为空")
    if len(labels) != len(values): raise ValueError("labels 和 values 长度必须一致")
    if colors is None: colors = _get_colors(len(labels))
    fig, ax = plt.subplots(figsize=figsize)
    total = sum(values)
    def make_autopct(vals):
        def autopct(pct):
            val = int(round(pct*total/100.0))
            return f'{pct:.1f}%\n({val})'
        return autopct
    wedges, texts, autotexts = ax.pie(
        values, labels=labels, autopct=make_autopct(values), colors=colors,
        startangle=90, pctdistance=0.75, labeldistance=1.1,
        wedgeprops={'edgecolor':'white','linewidth':1.5}, textprops={'fontsize':10})
    if autotexts:
        for at in autotexts: at.set_fontsize(9); at.set_fontweight('bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=18)
    legend_labels = [f'{l} ({v})' for l, v in zip(labels, values)]
    ax.legend(wedges, legend_labels, title="图例", loc="center left",
              bbox_to_anchor=(1,0,0.5,1), fontsize=9, framealpha=0.8)
    plt.tight_layout()
    return fig

def _build_scatter_fig(title, series_list, x_label, y_label, show_trendline, figsize):
    if not series_list: raise ValueError("series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        x_data = [_ensure_float(v) for v in series.get("x", [])]
        y_data = [_ensure_float(v) for v in series.get("y", [])]
        color = series.get("color", colors[i])
        marker_size = series.get("marker_size", 60)
        clean_points = [(x,y) for x,y in zip(x_data,y_data) if x is not None and y is not None]
        if not clean_points: continue
        cx, cy = zip(*clean_points)
        ax.scatter(cx, cy, c=color, label=name, s=marker_size, alpha=0.7, edgecolors='white', linewidth=0.5)
        if show_trendline and len(clean_points) >= 3:
            z = np.polyfit(cx, cy, 1); p = np.poly1d(z)
            x_sorted = sorted(cx)
            ax.plot(x_sorted, p(x_sorted), '--', color=color, linewidth=1.5, alpha=0.6)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    if x_label: ax.set_xlabel(x_label, fontsize=11)
    if y_label: ax.set_ylabel(y_label, fontsize=11)
    _apply_clean_style(ax)
    ax.legend(loc='upper right', fontsize=8, framealpha=0.8)
    plt.tight_layout()
    return fig

def _build_heatmap_fig(title, row_labels, col_labels, data_matrix, x_label, y_label,
                        color_scheme, annotate, figsize):
    data_array = np.array(data_matrix, dtype=float)
    fig, ax = plt.subplots(figsize=figsize)
    im = ax.imshow(data_array, cmap=color_scheme, aspect='auto')
    ax.set_xticks(range(len(col_labels))); ax.set_yticks(range(len(row_labels)))
    ax.set_xticklabels(col_labels, rotation=90, ha='center', fontsize=8)
    ax.set_yticklabels(row_labels, fontsize=9)
    if annotate and data_array.size <= 200:
        for i in range(len(row_labels)):
            for j in range(len(col_labels)):
                val = data_array[i,j]
                if not np.isnan(val):
                    tc = 'white' if abs(val) > (np.nanmax(data_array)-np.nanmin(data_array))/2 else 'black'
                    ax.text(j, i, _format_value(val,1), ha='center', va='center', fontsize=7, color=tc)
    cbar = plt.colorbar(im, ax=ax, shrink=0.85, pad=0.02)
    cbar.ax.tick_params(labelsize=8)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    if x_label: ax.set_xlabel(x_label, fontsize=11)
    if y_label: ax.set_ylabel(y_label, fontsize=11)
    plt.tight_layout()
    return fig

def _build_boxplot_fig(title, series_list, y_label, horizontal, figsize):
    if not series_list: raise ValueError("series_list 不能为空")
    colors = _get_colors(len(series_list))
    labels = [s.get("name", f"系列{i+1}") for i, s in enumerate(series_list)]
    data_groups = [[_ensure_float(v) or np.nan for v in s.get("data", [])] for s in series_list]
    box_colors = [s.get("color", colors[i]) for i, s in enumerate(series_list)]
    fig, ax = plt.subplots(figsize=figsize)
    if horizontal:
        bp = ax.boxplot(data_groups, tick_labels=labels, vert=False, patch_artist=True,
                         showmeans=True, meanprops=dict(marker='D', markerfacecolor='red', markersize=6))
        if y_label: ax.set_xlabel(y_label, fontsize=11)
    else:
        bp = ax.boxplot(data_groups, tick_labels=labels, vert=True, patch_artist=True,
                         showmeans=True, meanprops=dict(marker='D', markerfacecolor='red', markersize=6))
        if y_label: ax.set_ylabel(y_label, fontsize=11)
    for patch, color in zip(bp['boxes'], box_colors):
        patch.set_facecolor(color); patch.set_alpha(0.6)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    _apply_clean_style(ax)
    from matplotlib.lines import Line2D
    ax.legend(handles=[Line2D([0],[0], marker='D', color='w', markerfacecolor='red',
                               markersize=8, label='均值')], loc='upper right', fontsize=8)
    plt.tight_layout()
    return fig

def _build_area_fig(title, x_labels, series_list, y_label, stacked, alpha, figsize):
    if not series_list or not x_labels: raise ValueError("x_labels 和 series_list 不能为空")
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize)
    x_pos = range(len(x_labels))
    all_data = []
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        clean_data = np.array([_ensure_float(v) or 0 for v in data])
        all_data.append({"name":name,"data":clean_data,"unit":unit,"color":color})
    if stacked:
        y_stack = np.zeros(len(x_labels))
        for item in all_data:
            ax.fill_between(x_pos, y_stack, y_stack+item["data"], alpha=alpha, color=item["color"],
                            label=f'{item["name"]} ({item["unit"]})' if item["unit"] else item["name"])
            ax.plot(x_pos, y_stack+item["data"], color=item["color"], linewidth=1.5, alpha=0.9)
            y_stack = y_stack + item["data"]
    else:
        for item in all_data:
            ax.fill_between(x_pos, item["data"], alpha=alpha, color=item["color"],
                            label=f'{item["name"]} ({item["unit"]})' if item["unit"] else item["name"])
            ax.plot(x_pos, item["data"], color=item["color"], linewidth=1.5, alpha=0.9)
    ax.set_xticks(list(x_pos))
    if len(x_labels) > 20:
        step = max(1, len(x_labels)//15)
        ax.set_xticks(x_pos[::step])
        ax.set_xticklabels([x_labels[i] for i in x_pos[::step]], rotation=90, ha='center', fontsize=8)
    else:
        ax.set_xticklabels(x_labels, rotation=90, ha='center', fontsize=9)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    if y_label: ax.set_ylabel(y_label, fontsize=11)
    _apply_clean_style(ax)
    ax.legend(loc='upper left', fontsize=8, framealpha=0.8, ncol=min(3, len(series_list)))
    plt.tight_layout()
    return fig

def _build_dual_axis_fig(title, x_labels, left_series_list, right_series_list,
                          left_y_label, right_y_label, figsize):
    left_colors = _get_colors(len(left_series_list)) if left_series_list else []
    right_colors_base = ['#FF5722','#9C27B0','#E91E63','#795548','#607D8B']
    fig, ax1 = plt.subplots(figsize=figsize)
    x_pos = range(len(x_labels))
    lines_left = []
    for i, series in enumerate(left_series_list):
        name = series.get("name", f"左系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", left_colors[i] if left_colors else '#2196F3')
        clean_data = [_ensure_float(v) for v in data]
        clean_x = [j for j, v in enumerate(clean_data) if v is not None]
        clean_y = [clean_data[j] for j in clean_x]
        if not clean_y: continue
        label = f"{name} ({unit})" if unit else name
        line, = ax1.plot(clean_x, clean_y, marker='o', color=color, linewidth=2, markersize=5, label=label, alpha=0.9)
        lines_left.append(line)
    if left_y_label: ax1.set_ylabel(left_y_label, fontsize=11)
    ax1.set_xlabel('时间', fontsize=10)
    ax2 = ax1.twinx()
    lines_right = []
    for i, series in enumerate(right_series_list):
        name = series.get("name", f"右系列{i+1}")
        data = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", right_colors_base[i%len(right_colors_base)])
        clean_data = [_ensure_float(v) for v in data]
        clean_x = [j for j, v in enumerate(clean_data) if v is not None]
        clean_y = [clean_data[j] for j in clean_x]
        if not clean_y: continue
        label = f"{name} ({unit})" if unit else name
        line, = ax2.plot(clean_x, clean_y, marker='s', color=color, linewidth=2, markersize=5,
                         label=label, alpha=0.9, linestyle='--')
        lines_right.append(line)
    if right_y_label: ax2.set_ylabel(right_y_label, fontsize=11)
    ax1.set_xticks(list(x_pos))
    if len(x_labels) > 20:
        step = max(1, len(x_labels)//15)
        ax1.set_xticks(x_pos[::step])
        ax1.set_xticklabels([x_labels[i] for i in x_pos[::step]], rotation=90, ha='center', fontsize=8)
    else:
        ax1.set_xticklabels(x_labels, rotation=90, ha='center', fontsize=9)
    ax1.set_title(title, fontsize=14, fontweight='bold', pad=12)
    ax1.spines['top'].set_visible(False)
    ax1.grid(True, linestyle='--', alpha=0.3, color='gray')
    all_lines = lines_left + lines_right
    if all_lines:
        ax1.legend(all_lines, [l.get_label() for l in all_lines],
                   loc='upper left', fontsize=8, framealpha=0.8, ncol=min(3, len(all_lines)))
    plt.tight_layout()
    return fig

def _build_radar_fig(title, categories, series_list, fill, figsize):
    n = len(categories)
    angles = np.linspace(0, 2*np.pi, n, endpoint=False).tolist()
    angles += angles[:1]
    colors = _get_colors(len(series_list))
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': 'polar'})
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        data = series.get("data", [])
        if len(data) != n:
            raise ValueError(f"系列 '{name}' 的 data 长度 ({len(data)}) 与 categories ({n}) 不一致")
        color = series.get("color", colors[i])
        clean_data = [_ensure_float(v) or 0 for v in data]
        clean_data += clean_data[:1]
        ax.plot(angles, clean_data, 'o-', color=color, linewidth=2, markersize=6, label=name)
        if fill: ax.fill(angles, clean_data, alpha=0.15, color=color)
    ax.set_xticks(angles[:-1]); ax.set_xticklabels(categories, fontsize=11)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=25)
    ax.legend(loc='upper right', bbox_to_anchor=(1.2, 1.1), fontsize=8, framealpha=0.8)
    ax.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    return fig

def _build_energy_fig(title, daily_labels, daily_values, hourly_labels, hourly_values, unit, figsize):
    fig, ax1 = plt.subplots(1, 1, figsize=figsize)
    clean_daily = [_ensure_float(v) or 0 for v in daily_values]
    bars = ax1.bar(range(len(daily_labels)), clean_daily,
                   color='#2196F3', alpha=0.8, edgecolor='white', linewidth=0.5)
    for bar, val in zip(bars, clean_daily):
        ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+max(clean_daily)*0.02,
                 _format_value(val,1), ha='center', fontsize=8, color='#333')
    if len(clean_daily) > 1:
        z = np.polyfit(range(len(clean_daily)), clean_daily, 1)
        p = np.poly1d(z)
        ax1.plot(range(len(clean_daily)), p(range(len(clean_daily))),
                 '--', color='#FF5722', linewidth=2, alpha=0.7, label='趋势线')
    ax1.set_xticks(range(len(daily_labels)))
    ax1.set_xticklabels(daily_labels, rotation=90, ha='center', fontsize=9)
    ax1.set_ylabel(f'能耗 ({unit})', fontsize=11)
    ax1.set_title(title, fontsize=13, fontweight='bold')
    _apply_clean_style(ax1)
    if len(clean_daily) > 1: ax1.legend(fontsize=8)
    plt.tight_layout()
    return fig

# ============================================================
# Plotly 交互式图表
# ============================================================
def _plotly_trend(title, data):
    x_labels = data.get("x_labels", [])
    series_list = data.get("series_list", [])
    y_label = data.get("y_label", "")
    fig = go.Figure()
    colors = _get_colors(len(series_list))
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        sdata = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        label = f"{name} ({unit})" if unit else name
        fig.add_trace(go.Scatter(x=x_labels, y=sdata, mode='lines+markers',
                                  name=label, line=dict(color=color, width=2),
                                  marker=dict(size=6)))
    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),
        xaxis=dict(title='时间', tickangle=45),
        yaxis=dict(title=y_label) if y_label else {},
        hovermode='x unified', template='plotly_white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_bar(title, data):
    categories = data.get("categories", [])
    series_list = data.get("series_list", [])
    y_label = data.get("y_label", "")
    horizontal = data.get("horizontal", False)
    fig = go.Figure()
    colors = _get_colors(len(series_list))
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        sdata = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        label = f"{name} ({unit})" if unit else name
        orientation = 'h' if horizontal else 'v'
        fig.add_trace(go.Bar(x=sdata if horizontal else categories,
                              y=categories if horizontal else sdata,
                              name=label, marker_color=color, orientation=orientation))
    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),
        barmode='group', template='plotly_white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_pie(title, data):
    labels = data.get("labels", [])
    values = data.get("values", [])
    colors = data.get("colors") or _get_colors(len(labels))
    donut = data.get("donut", False)
    fig = go.Figure(data=[go.Pie(labels=labels, values=values,
                                  marker=dict(colors=colors),
                                  hole=0.4 if donut else 0,
                                  textinfo='label+percent+value')])
    fig.update_layout(title=dict(text=title, font=dict(size=18)), template='plotly_white')
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_scatter(title, data):
    series_list = data.get("series_list", [])
    x_label = data.get("x_label", "")
    y_label = data.get("y_label", "")
    fig = go.Figure()
    colors = _get_colors(len(series_list))
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        color = series.get("color", colors[i])
        fig.add_trace(go.Scatter(x=series.get("x",[]), y=series.get("y",[]),
                                  mode='markers', name=name,
                                  marker=dict(color=color, size=10, opacity=0.7)))
    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),
        xaxis=dict(title=x_label) if x_label else {},
        yaxis=dict(title=y_label) if y_label else {},
        template='plotly_white')
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_heatmap(title, data):
    fig = go.Figure(data=go.Heatmap(
        z=data.get("data_matrix",[]), x=data.get("col_labels",[]), y=data.get("row_labels",[]),
        colorscale='RdYlBu_r'))
    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),
        xaxis=dict(title=data.get("x_label",""), tickangle=45),
        yaxis=dict(title=data.get("y_label","")),
        template='plotly_white')
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_area(title, data):
    x_labels = data.get("x_labels", [])
    series_list = data.get("series_list", [])
    y_label = data.get("y_label", "")
    stacked = data.get("stacked", True)
    fig = go.Figure()
    colors = _get_colors(len(series_list))
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        sdata = series.get("data", [])
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        label = f"{name} ({unit})" if unit else name
        fig.add_trace(go.Scatter(x=x_labels, y=sdata, mode='lines', name=label,
                                  fill='tonexty' if stacked and i > 0 else 'tozeroy',
                                  line=dict(color=color, width=2)))
    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),
        xaxis=dict(title='时间', tickangle=45),
        yaxis=dict(title=y_label) if y_label else {},
        template='plotly_white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_dual_axis(title, data):
    x_labels = data.get("x_labels", [])
    left_series_list = data.get("left_series_list", [])
    right_series_list = data.get("right_series_list", [])
    left_y_label = data.get("left_y_label", "")
    right_y_label = data.get("right_y_label", "")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    colors = _get_colors(len(left_series_list))
    for i, series in enumerate(left_series_list):
        name = series.get("name", f"左系列{i+1}")
        unit = series.get("unit", "")
        color = series.get("color", colors[i])
        label = f"{name} ({unit})" if unit else name
        fig.add_trace(go.Scatter(x=x_labels, y=series.get("data",[]),
                                  mode='lines+markers', name=label,
                                  line=dict(color=color, width=2), marker=dict(size=6)),
                      secondary_y=False)
    right_colors = ['#FF5722','#9C27B0','#E91E63','#795548','#607D8B']
    for i, series in enumerate(right_series_list):
        name = series.get("name", f"右系列{i+1}")
        unit = series.get("unit", "")
        color = series.get("color", right_colors[i%len(right_colors)])
        label = f"{name} ({unit})" if unit else name
        fig.add_trace(go.Scatter(x=x_labels, y=series.get("data",[]),
                                  mode='lines+markers', name=label,
                                  line=dict(color=color, width=2, dash='dash'),
                                  marker=dict(size=6, symbol='square')),
                      secondary_y=True)
    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),
        xaxis=dict(title='时间', tickangle=45), template='plotly_white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        hovermode='x unified')
    fig.update_yaxes(title_text=left_y_label, secondary_y=False)
    fig.update_yaxes(title_text=right_y_label, secondary_y=True)
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_boxplot(title, data):
    series_list = data.get("series_list", [])
    y_label = data.get("y_label", "")
    horizontal = data.get("horizontal", False)
    fig = go.Figure()
    colors = _get_colors(len(series_list))
    for i, series in enumerate(series_list):
        name = series.get("name", f"系列{i+1}")
        sdata = series.get("data", [])
        color = series.get("color", colors[i])
        fig.add_trace(go.Box(y=sdata if not horizontal else None,
                              x=sdata if horizontal else None,
                              name=name, marker_color=color, boxmean='sd'))
    fig.update_layout(title=dict(text=title, font=dict(size=18)), template='plotly_white')
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def _plotly_gauge(title, data):
    value = data.get("value", 0)
    min_val = data.get("min_val", 0)
    max_val = data.get("max_val", 100)
    unit = data.get("unit", "")
    thresholds = data.get("thresholds")
    steps = []
    if thresholds:
        for th in thresholds:
            rng = th.get("range", [0, 0])
            steps.append({'range': [(rng[0]-min_val)/(max_val-min_val),
                                     (rng[1]-min_val)/(max_val-min_val)],
                          'color': th.get('color', '#ccc')})
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=value, title={'text': title},
        number={'suffix': f' {unit}' if unit else '', 'font': {'size': 32}},
        gauge={'axis': {'range': [min_val, max_val]}, 'bar': {'color': '#333'},
               'steps': steps, 'threshold': {'line': {'color': 'red', 'width': 3},
                                              'thickness': 0.75, 'value': value}}))
    fig.update_layout(template='plotly_white', height=350)
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

# ============================================================
# 统一入口: generate_chart_from_data（matplotlib 引擎）
# ============================================================
def generate_chart_from_data(chart_type, title, data, figsize=None, dpi=120):
    _ensure_font()
    chart_type = chart_type.lower().strip()
    dispatch = {
        "trend":     lambda: generate_trend_chart(title=title, x_labels=data.get("x_labels",[]),
                          series_list=data.get("series_list",[]), y_label=data.get("y_label",""),
                          figsize=figsize or (12,6), dpi=dpi),
        "bar":       lambda: generate_bar_chart(title=title, categories=data.get("categories",[]),
                          series_list=data.get("series_list",[]), y_label=data.get("y_label",""),
                          figsize=figsize or (10,6), dpi=dpi, horizontal=data.get("horizontal",False)),
        "pie":       lambda: generate_pie_chart(title=title, labels=data.get("labels",[]),
                          values=data.get("values",[]), colors=data.get("colors"),
                          figsize=figsize or (8,6), dpi=dpi, donut=data.get("donut",False)),
        "scatter":   lambda: generate_scatter_chart(title=title, series_list=data.get("series_list",[]),
                          x_label=data.get("x_label",""), y_label=data.get("y_label",""),
                          figsize=figsize or (10,6), dpi=dpi,
                          show_trendline=data.get("show_trendline",True)),
        "heatmap":   lambda: generate_heatmap_chart(title=title, row_labels=data.get("row_labels",[]),
                          col_labels=data.get("col_labels",[]), data_matrix=data.get("data_matrix",[]),
                          x_label=data.get("x_label",""), y_label=data.get("y_label",""),
                          figsize=figsize or (12,8), dpi=dpi,
                          color_scheme=data.get("color_scheme","coolwarm"),
                          annotate=data.get("annotate",True)),
        "boxplot":   lambda: generate_boxplot_chart(title=title, series_list=data.get("series_list",[]),
                          y_label=data.get("y_label",""), figsize=figsize or (10,6), dpi=dpi,
                          horizontal=data.get("horizontal",False)),
        "area":      lambda: generate_area_chart(title=title, x_labels=data.get("x_labels",[]),
                          series_list=data.get("series_list",[]), y_label=data.get("y_label",""),
                          figsize=figsize or (12,6), dpi=dpi,
                          stacked=data.get("stacked",True), alpha=data.get("alpha",0.6)),
        "gauge":     lambda: generate_gauge_chart(title=title, value=data.get("value",0),
                          min_val=data.get("min_val",0), max_val=data.get("max_val",100),
                          unit=data.get("unit",""), thresholds=data.get("thresholds"),
                          figsize=figsize or (8,6), dpi=dpi),
        "dual_axis": lambda: generate_dual_axis_chart(title=title, x_labels=data.get("x_labels",[]),
                          left_series_list=data.get("left_series_list",[]),
                          right_series_list=data.get("right_series_list",[]),
                          left_y_label=data.get("left_y_label",""),
                          right_y_label=data.get("right_y_label",""),
                          figsize=figsize or (12,6), dpi=dpi),
        "radar":     lambda: generate_radar_chart(title=title, categories=data.get("categories",[]),
                          series_list=data.get("series_list",[]),
                          figsize=figsize or (8,8), dpi=dpi, fill=data.get("fill",True)),
        "energy":    lambda: generate_energy_chart(title=title, daily_labels=data.get("daily_labels",[]),
                          daily_values=data.get("daily_values",[]),
                          hourly_labels=data.get("hourly_labels"),
                          hourly_values=data.get("hourly_values"),
                          unit=data.get("unit","kWh"), figsize=figsize or (14,6), dpi=dpi),
    }
    if chart_type not in dispatch:
        raise ValueError(f"不支持的图表类型: '{chart_type}'。支持: {', '.join(SUPPORTED_CHART_TYPES)}")
    return dispatch[chart_type]()

# ============================================================
# 统一入口: generate_chart_with_file（返回 base64 + 文件路径）
# ============================================================
def generate_chart_with_file(chart_type, title, data, figsize=None, dpi=80):
    _ensure_font()
    _cleanup_old_charts()
    chart_type = chart_type.lower().strip()
    builders = {
        "trend": lambda: _build_trend_fig(title, data.get("x_labels",[]),
                          data.get("series_list",[]), data.get("y_label",""),
                          figsize or (10,5)),
        "bar": lambda: _build_bar_fig(title, data.get("categories",[]),
                          data.get("series_list",[]), data.get("y_label",""),
                          data.get("horizontal",False), figsize or (8,5)),
        "pie": lambda: _build_pie_fig(title, data.get("labels",[]),
                          data.get("values",[]), data.get("colors"), figsize or (7,5)),
        "scatter": lambda: _build_scatter_fig(title, data.get("series_list",[]),
                          data.get("x_label",""), data.get("y_label",""),
                          data.get("show_trendline",True), figsize or (10,6)),
        "heatmap": lambda: _build_heatmap_fig(title, data.get("row_labels",[]),
                          data.get("col_labels",[]), data.get("data_matrix",[]),
                          data.get("x_label",""), data.get("y_label",""),
                          data.get("color_scheme","coolwarm"), data.get("annotate",True),
                          figsize or (12,8)),
        "boxplot": lambda: _build_boxplot_fig(title, data.get("series_list",[]),
                          data.get("y_label",""), data.get("horizontal",False),
                          figsize or (10,6)),
        "area": lambda: _build_area_fig(title, data.get("x_labels",[]),
                          data.get("series_list",[]), data.get("y_label",""),
                          data.get("stacked",True), data.get("alpha",0.6),
                          figsize or (12,6)),
        "dual_axis": lambda: _build_dual_axis_fig(title, data.get("x_labels",[]),
                          data.get("left_series_list",[]), data.get("right_series_list",[]),
                          data.get("left_y_label",""), data.get("right_y_label",""),
                          figsize or (12,6)),
        "radar": lambda: _build_radar_fig(title, data.get("categories",[]),
                          data.get("series_list",[]), data.get("fill",True),
                          figsize or (8,8)),
        "energy": lambda: _build_energy_fig(title, data.get("daily_labels",[]),
                          data.get("daily_values",[]), data.get("hourly_labels"),
                          data.get("hourly_values"), data.get("unit","kWh"),
                          figsize or (12,7)),
    }
    if chart_type == "gauge":
        figsize_g = figsize or (8,6)
        thresholds_data = data.get("thresholds")
        if thresholds_data is None:
            thresholds_data = [
                {"label":"正常","range":[0,60],"color":"#4CAF50"},
                {"label":"注意","range":[60,85],"color":"#FF9800"},
                {"label":"报警","range":[85,100],"color":"#FF5722"},
            ]
        fig = _build_gauge_fig(title, data.get("value",0), data.get("min_val",0),
                                data.get("max_val",100), data.get("unit",""),
                                thresholds_data, figsize_g)
        file_path = _fig_to_file(fig, dpi)
        with open(file_path, 'rb') as f:
            img_data = f.read()
        return {"chart_base64": base64.b64encode(img_data).decode('utf-8'), "file_path": file_path}
    if chart_type not in builders:
        raise ValueError(f"不支持的图表类型: '{chart_type}'。支持: {', '.join(SUPPORTED_CHART_TYPES)}")
    fig = builders[chart_type]()
    file_path,file_path1 = _fig_to_file(fig, dpi)
    with open(file_path, 'rb') as f:
        img_data = f.read()
    return {"chart_base64": base64.b64encode(img_data).decode('utf-8'), "file_path": file_path1}

def _build_gauge_fig(title, value, min_val, max_val, unit, thresholds, figsize):
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': 'polar'})
    ax.set_theta_zero_location('N'); ax.set_theta_direction(-1)
    ax.set_thetamin(0); ax.set_thetamax(180)
    total_range = max_val - min_val
    for th in thresholds:
        rng = th["range"]
        start_angle = (rng[0]-min_val)/total_range*180
        end_angle = (rng[1]-min_val)/total_range*180
        theta = np.linspace(start_angle, end_angle, 50)
        theta_rad = np.radians(theta)
        ax.fill_between(theta_rad, np.full_like(theta_rad,1.0), np.full_like(theta_rad,1.4),
                        color=th["color"], alpha=0.6)
        mid_theta = np.radians((start_angle+end_angle)/2)
        ax.text(mid_theta, 1.55, th.get("label",""), ha='center', va='center',
                fontsize=8, fontweight='bold', color=th["color"])
    pointer_angle = (value-min_val)/total_range*180
    pointer_rad = np.radians(pointer_angle)
    ax.annotate('', xy=(pointer_rad,1.3), xytext=(pointer_rad,0.2),
                arrowprops=dict(arrowstyle='->', color='#333', lw=3))
    ax.scatter(pointer_rad, 0.2, s=150, color='#333', zorder=10)
    tick_angles = np.linspace(0, 180, 9); tick_rad = np.radians(tick_angles)
    tick_values = np.linspace(min_val, max_val, 9)
    for ta, tv in zip(tick_rad, tick_values):
        ax.plot([ta, ta], [0.9, 1.0], color='gray', linewidth=1)
        ax.text(ta, 0.75, f'{tv:.0f}', ha='center', va='center', fontsize=7, color='gray')
    display_text = f'{_format_value(value,1)} {unit}' if unit else f'{_format_value(value,1)}'
    ax.text(np.radians(90), 0.15, display_text, ha='center', va='center',
            fontsize=28, fontweight='bold', color='#333')
    ax.text(np.radians(90), -0.05, title, ha='center', va='center', fontsize=12, color='#666')
    ax.set_ylim(0, 1.7); ax.set_xticklabels([]); ax.set_yticklabels([])
    ax.grid(False); ax.spines.clear()
    plt.tight_layout()
    return fig

# ============================================================
# Plotly 统一入口
# ============================================================
def generate_plotly_chart(chart_type, title, data, engine="plotly"):
    if not _PLOTLY_AVAILABLE:
        return {"success": False, "error": "Plotly 未安装",
                "message": "请安装 plotly: pip install plotly", "html": "", "html_file": ""}
    _ensure_font()
    chart_type = chart_type.lower().strip()
    html_str = ""
    try:
        dispatch = {
            "trend": _plotly_trend, "bar": _plotly_bar, "pie": _plotly_pie,
            "scatter": _plotly_scatter, "heatmap": _plotly_heatmap, "area": _plotly_area,
            "dual_axis": _plotly_dual_axis, "boxplot": _plotly_boxplot, "gauge": _plotly_gauge,
        }
        if chart_type not in dispatch:
            raise ValueError(f"Plotly 不支持的图表类型: '{chart_type}'")
        html_str = dispatch[chart_type](title, data)
        html_filename = f"chart_{int(time.time()*1000)}_{os.urandom(4).hex()}.html"
        html_filepath = os.path.join(CHART_OUTPUT_DIR, html_filename)
        with open(html_filepath, 'w', encoding='utf-8') as f:
            f.write(html_str)
        return {"success": True, "html": html_str, "html_file": html_filepath,
                "chart_type": chart_type, "title": title, "format": "html"}
    except Exception as e:
        logger.error(f"Plotly 图表生成失败: {e}", exc_info=True)
        return {"success": False, "error": str(e), "html": "", "html_file": "",
                "chart_type": chart_type, "title": title}

# ============================================================
# 获取支持的图表类型
# ============================================================
def get_supported_chart_types():
    return {
        "chart_types": CHART_TYPE_MAP,
        "supported_types": SUPPORTED_CHART_TYPES,
        "plotly_available": _PLOTLY_AVAILABLE,
        "engine": "matplotlib (默认) + plotly (可选)",
        "description": "通用图表生成模块，支持11种图表类型，可被任何子系统 MCP 调用"
    }
