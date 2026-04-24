import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

fig, ax = plt.subplots(1, 1, figsize=(18, 30))
ax.set_xlim(0, 18)
ax.set_ylim(0, 30)
ax.axis('off')
fig.patch.set_facecolor('white')

C_START = '#1a365d'
C_PROC = '#e2e8f0'
C_DEC = '#fef3c7'
C_YES = '#dcfce7'
C_NO = '#fee2e2'
C_LOOP = '#ede9fe'
C_OUT = '#dbeafe'
C_ARR = '#475569'
C_TXT = '#1e293b'

def box(x, y, w, h, text, color, border='#94a3b8', fs=8, bold=False):
    r = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.2", facecolor=color, edgecolor=border, linewidth=1.2)
    ax.add_patch(r)
    ax.text(x+w/2, y+h/2, text, ha='center', va='center', fontsize=fs, color=C_TXT,
            fontweight='bold' if bold else 'normal', linespacing=1.3, fontfamily='sans-serif')

def diamond(x, y, w, h, text, color=C_DEC, fs=7.5):
    pts = [(x+w/2, y+h), (x+w, y+h/2), (x+w/2, y), (x, y+h/2)]
    ax.add_patch(plt.Polygon(pts, facecolor=color, edgecolor='#d97706', linewidth=1.2))
    ax.text(x+w/2, y+h/2, text, ha='center', va='center', fontsize=fs, color=C_TXT, fontweight='bold', linespacing=1.2)

def arr(x1, y1, x2, y2, label='', side='right', lc=None):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle='->', color=C_ARR, lw=1.5))
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2
        c = lc or ('#059669' if 'YES' in label.upper() else '#dc2626')
        ha = 'left' if side == 'right' else 'right'
        off = 0.15 if side == 'right' else -0.15
        ax.text(mx+off, my, label, fontsize=7, color=c, fontweight='bold', ha=ha, va='center')

# ═══ FLOWCHART ═══
Y = 29.2
box(7.5, Y, 3, 0.5, 'START', C_START, C_START, fs=10, bold=True)
ax.texts[-1].set_color('white')

Y -= 1; arr(9, 29.2, 9, Y+0.7)
box(5, Y-0.1, 8, 0.7, 'Step 8.1: Create ARS_ALLOC_WORKING\nExpand options to variant x size (join ARS_MSA_VAR_ART)\nFilter: ALLOC_FLAG=1 AND FNL_Q > 0', C_PROC, fs=7.5)

Y -= 1.2; arr(9, Y+0.8, 9, Y+0.7)
diamond(7, Y-0.1, 4, 0.8, 'Rows > 0?')
arr(11, Y+0.3, 13.5, Y+0.3, 'NO')
box(13.5, Y+0.05, 2.5, 0.5, 'EXIT\n(No eligible rows)', C_NO, fs=7)
Y -= 1.2; arr(9, Y+0.8, 9, Y+0.6, 'YES')

box(5, Y-0.15, 8, 0.8, 'Step 8.2: Enrich\nSTK_TTL from VAR_ART grid (store variant stock)\nCONT from Master_CONT_SZ (ST -> CO -> 1/count)\nSZ_MBQ = OPT_MBQ x CONT\nSZ_REQ = MAX(0, SZ_MBQ - STK_TTL)', C_PROC, fs=7)

Y -= 1.2; arr(9, Y+0.7, 9, Y+0.5)
box(5, Y-0.1, 8, 0.6, 'Step 8.3-4: Add tracking columns (ALLOC_QTY, SKIP_FLAG...)\nCreate Pool: #alloc_pool with FNL_Q_REM per (RDC,GEN_ART,CLR,VAR,SZ)', C_PROC, fs=7)

Y -= 1.1; arr(9, Y+0.6, 9, Y+0.5)
box(5, Y-0.15, 8, 0.7, 'Step 8.5: Mark Eligibility (E1-E5)\nE3:OPT_TYPE!=MIX  E1:LISTING=1  E2:ALLOC_FLAG=1\nE4:MSA_FNL_Q>0  E5:OPT_REQ_WH>=1 (or FOCUS)', C_OUT, fs=7)

Y -= 1.1; arr(9, Y+0.55, 9, Y+0.45)
diamond(7, Y-0.2, 4, 0.8, 'Any E1-E5\nfails?')
arr(11, Y+0.2, 13.5, Y+0.2, 'YES (fail)')
box(13.5, Y-0.05, 3, 0.5, 'INELIGIBLE\n+ reason (E1/E2/E3/E4/E5)', C_NO, fs=7)
Y -= 1.2; arr(9, Y+0.6, 9, Y+0.5, 'NO (all pass)')

# MAIN LOOP
box(3.5, Y-0.1, 11, 0.5, 'FOR EACH OPT_TYPE IN [ RL -> TBC -> TBL ]', C_LOOP, '#7c3aed', fs=9, bold=True)
Y -= 0.8; arr(9, Y+0.4, 9, Y+0.25)
box(4, Y-0.1, 10, 0.45, 'FOR EACH I_ROD ROUND (1, 2, ... max_I_ROD)', C_LOOP, '#a78bfa', fs=8, bold=True)

Y -= 0.8; arr(9, Y+0.35, 9, Y+0.2)
box(5.5, Y-0.2, 7, 0.55, 'A: Scale demand\nSZ_MBQ = OPT_MBQ x round_N x CONT\nSZ_REQ = MAX(0, SZ_MBQ - STK_TTL - prev_ALLOC)', C_PROC, fs=7)

Y -= 0.9; arr(9, Y+0.35, 9, Y+0.2)
box(5, Y-0.25, 8, 0.7, 'B: Waterfall allocate (1 SQL, ALL stores at once)\nPriority: FOCUS_WO_CAP -> FOCUS_W_CAP -> ST_RANK\nEach store gets MIN(SZ_REQ, pool remaining)\nPool shared: highest-priority stores served first', C_PROC, fs=7)

Y -= 0.8; arr(9, Y+0.2, 9, Y+0.1)
box(6, Y-0.1, 6, 0.35, 'C: Deduct pool (FNL_Q_REM -= allocated)', C_PROC, fs=7)

Y -= 0.85; arr(9, Y+0.4, 9, Y+0.3)
diamond(7, Y-0.2, 4, 0.75, 'OPT_TYPE\n= TBL?')

# TBL YES path
arr(11, Y+0.18, 13.5, Y+0.18, 'YES')
diamond(13.2, Y-0.2, 3.5, 0.75, 'avail_sizes /\ntotal_sizes\n< 60%?')
arr(15, Y-0.2, 15, Y-0.9, 'YES (break!)', side='right', lc='#dc2626')
box(13.5, Y-1.35, 3, 0.45, 'RESTORE pool for stores\n>= break_rank. SKIP_FLAG=1', C_NO, fs=7)
ax.text(16.8, Y+0.18, 'NO -> continue', fontsize=7, color='#059669', fontweight='bold', va='center')

# TBL NO path (RL/TBC skip validation)
Y -= 0.5; arr(9, Y+0.2, 9, Y+0.05, 'NO (RL/TBC)', lc='#059669')

Y -= 0.5
box(5.5, Y-0.15, 7, 0.5, 'E: Commit ALLOC_QTY += ROUND_ALLOC\nSet: ALLOCATED (full) / PARTIAL (underfilled)', C_PROC, fs=7)

Y -= 0.8; arr(9, Y+0.45, 9, Y+0.35)
box(5, Y-0.2, 8, 0.6, 'F: Post-sync to ARS_LISTING_WORKING\nB1: MSA_FNL_Q = current pool remaining\nB2: OPT_REQ_WH = recalc (target - stock - allocated)\nB3: VAR_FNL_COUNT = variants still in pool', C_OUT, fs=7)

Y -= 0.7; arr(9, Y+0.1, 9, Y)
box(5, Y-0.2, 8, 0.35, 'END ROUND -> next I_ROD (demand scales up x N)', C_LOOP, '#a78bfa', fs=7)

Y -= 0.55; arr(9, Y+0.05, 9, Y-0.05)
box(4, Y-0.25, 10, 0.35, 'END OPT_TYPE -> next wave (RL done -> TBC -> TBL)', C_LOOP, '#7c3aed', fs=8, bold=True)

# Fallback
Y -= 0.85; arr(9, Y+0.25, 9, Y+0.15)
diamond(7, Y-0.3, 4, 0.8, 'enable_fallback\n= TRUE?')

arr(11, Y+0.1, 13.5, Y+0.1, 'YES')
box(13.5, Y-0.25, 3.5, 0.7, 'FALLBACK:\n1. Demote grid (Pri->Sec)\n2. Recalc PRI_CT%\n3. Boost OPT_MBQ x 130%\n4. Re-run new eligible only', C_YES, fs=6.5)

Y -= 1.1; arr(9, Y+0.5, 9, Y+0.35, 'NO')

box(5.5, Y-0.15, 7, 0.55, 'Reflect ALLOC_QTY -> ARS_LISTING_WORKING\nSet FINAL_OPT_TYPE:\n  ALLOCATED / PARTIAL / SKIP', C_OUT, fs=7.5)

Y -= 0.8; arr(9, Y+0.25, 9, Y+0.15)
box(6.5, Y-0.1, 5, 0.35, 'Cleanup: Drop pool + break tables', C_PROC, fs=7)

Y -= 0.65; arr(9, Y+0.2, 9, Y+0.1)
box(7, Y-0.15, 4, 0.5, 'DONE\nalloc_rows | skipped | ineligible | duration', C_START, C_START, fs=8, bold=True)
ax.texts[-1].set_color('white')

# Legend
LY = 0.8
ax.text(0.5, LY+0.5, 'LEGEND:', fontsize=8, fontweight='bold', color=C_TXT)
for i, (c, l) in enumerate([(C_PROC,'Process'), (C_DEC,'Decision'), (C_YES,'Yes/True'),
                              (C_NO,'No/False/Error'), (C_LOOP,'Loop'), (C_OUT,'Output/Sync')]):
    r = FancyBboxPatch((0.5+i*2.7, LY), 0.4, 0.25, boxstyle="round,pad=0.08", facecolor=c, edgecolor='#94a3b8', linewidth=0.8)
    ax.add_patch(r)
    ax.text(1.1+i*2.7, LY+0.12, l, fontsize=7, color=C_TXT, va='center')

plt.tight_layout()
plt.savefig(r'D:\ars\Part8_Allocation_Flowchart.png', dpi=150, bbox_inches='tight', facecolor='white')
print('Saved: Part8_Allocation_Flowchart.png')
