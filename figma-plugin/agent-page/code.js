// AGENT 页面 → Figma 图板生成插件
// 运行后：新建「AGENT · agent.html」页面，生成 01 首页态 / 02 对话态 两块 1440×900 画板

const W = 1440, H = 900, SB = 278, TOP = 64, BOT = 42;

const C = {
  bg: '#05060B', panel: '#0A0C13', composer: '#12151F', bar: '#080A11',
  t1: '#F2F5FB', t2: '#C2CADB', t3: '#98A3B8',
  w60: '#7B8698', w50: '#6B768C', dim: '#A9B3C7',
  white: '#FFFFFF', violet: '#8A7BFF', accent: '#8FB4FF', green: '#63E6A8'
};

/* ---------------- 基础工具 ---------------- */

function rgb(hex) {
  const h = hex.replace('#', '');
  return { r: parseInt(h.slice(0, 2), 16) / 255, g: parseInt(h.slice(2, 4), 16) / 255, b: parseInt(h.slice(4, 6), 16) / 255 };
}
function solid(hex, a) {
  return { type: 'SOLID', color: rgb(hex), opacity: a == null ? 1 : a };
}

let FONT = { regular: { family: 'Inter', style: 'Regular' }, bold: { family: 'Inter', style: 'Bold' } };

const FONT_CANDIDATES = ['Microsoft YaHei', 'Noto Sans SC', 'PingFang SC', 'Microsoft YaHei UI', 'Segoe UI', 'Inter', 'Roboto'];

async function pickFonts() {
  for (const fam of FONT_CANDIDATES) {
    try {
      await figma.loadFontAsync({ family: fam, style: 'Regular' });
    } catch (e) { continue; }
    let boldStyle = 'Bold';
    try {
      await figma.loadFontAsync({ family: fam, style: 'Bold' });
    } catch (e) {
      try { await figma.loadFontAsync({ family: fam, style: 'Semi Bold' }); boldStyle = 'Semi Bold'; }
      catch (e2) { boldStyle = 'Regular'; }
    }
    FONT = { regular: { family: fam, style: 'Regular' }, bold: { family: fam, style: boldStyle } };
    return;
  }
}

function frameNode(parent, o) {
  const f = figma.createFrame();
  f.name = o.name || 'frame';
  f.resize(o.w, o.h);
  f.fills = o.fill ? [solid(o.fill, o.alpha)] : [];
  f.cornerRadius = o.radius || 0;
  if (o.stroke) {
    f.strokes = [solid(o.stroke, o.strokeAlpha)];
    f.strokeWeight = o.strokeWeight || 1;
    f.strokeAlign = 'INSIDE';
  }
  f.clipsContent = o.clip !== false;
  parent.appendChild(f);
  f.x = o.x || 0;
  f.y = o.y || 0;
  if (o.corners) {
    f.topLeftRadius = o.corners[0]; f.topRightRadius = o.corners[1];
    f.bottomRightRadius = o.corners[2]; f.bottomLeftRadius = o.corners[3];
  }
  return f;
}

function makeText(parent, o) {
  const t = figma.createText();
  t.fontName = (o.weight === 'bold') ? FONT.bold : FONT.regular;
  t.characters = o.str;
  t.fontSize = o.size || 14;
  t.fills = [solid(o.color || C.t1, o.alpha)];
  if (o.ls) t.letterSpacing = { unit: 'PERCENT', value: o.ls };
  if (o.lh) t.lineHeight = { unit: 'PIXELS', value: o.lh };
  if (o.width) {
    t.textAutoResize = 'HEIGHT';
    t.resize(o.width, t.height);
    t.textAlignHorizontal = o.align || 'LEFT';
  } else {
    t.textAutoResize = 'WIDTH_AND_HEIGHT';
    if (o.align) t.textAlignHorizontal = o.align;
  }
  t.name = o.name || (o.str.length > 14 ? o.str.slice(0, 14) : o.str);
  parent.appendChild(t);
  t.x = o.x || 0;
  t.y = o.y || 0;
  if (o.centerX != null) t.x = Math.round(o.centerX - t.width / 2);
  return t;
}

const ICONS = {
  search: ['<circle cx="11" cy="11" r="7"/><path d="m20 20-3.6-3.6"/>', 2],
  compass: ['<circle cx="12" cy="12" r="9"/><path d="m15.5 8.5-2 5-5 2 2-5z"/>', 1.8],
  plus: ['<circle cx="12" cy="12" r="9"/><path d="M12 8v8M8 12h8"/>', 1.8],
  robot: ['<rect x="4" y="7" width="16" height="12" rx="4"/><path d="M9 12h.01M15 12h.01M12 3v4"/>', 1.8],
  star: ['<path d="M12 3.5l2.2 5.3 5.7.5-4.4 3.8 1.3 5.6L12 15.6 7.2 18.7l1.3-5.6L4.1 9.3l5.7-.5z"/>', 1.8],
  clock: ['<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 1.8"/>', 1.8],
  book: ['<path d="M4 5.5A1.5 1.5 0 0 1 5.5 4H10a2 2 0 0 1 2 2v14a2 2 0 0 0-2-2H5.5A1.5 1.5 0 0 1 4 16.5z"/><path d="M20 5.5A1.5 1.5 0 0 0 18.5 4H14a2 2 0 0 0-2 2v14a2 2 0 0 1 2-2h4.5a1.5 1.5 0 0 0 1.5-1.5z"/>', 1.8],
  grid: ['<rect x="4" y="4" width="7" height="7" rx="2"/><rect x="13" y="4" width="7" height="7" rx="2"/><rect x="4" y="13" width="7" height="7" rx="2"/><rect x="13" y="13" width="7" height="7" rx="2"/>', 1.8],
  gear: ['<circle cx="12" cy="12" r="3"/><path d="M12 3v2.5M12 18.5V21M3 12h2.5M18.5 12H21M5.6 5.6l1.8 1.8M16.6 16.6l1.8 1.8M18.4 5.6l-1.8 1.8M7.4 16.6l-1.8 1.8"/>', 1.8],
  chevLeft: ['<path d="M15 6l-6 6 6 6"/>', 2],
  chevDown: ['<path d="m6 9 6 6 6-6"/>', 2],
  send: ['<path d="M12 19V5M5 12l7-7 7 7"/>', 2.4],
  bell: ['<path d="M18 9a6 6 0 1 0-12 0c0 5-2 6.4-2 6.4h16S18 14 18 9"/><path d="M10.3 20a2 2 0 0 0 3.4 0"/>', 1.8],
  devices: ['<rect x="2.4" y="4" width="11.6" height="8.6" rx="2"/><path d="M5.8 17h5M8.4 12.6V17"/><rect x="15.4" y="9" width="6.2" height="10" rx="1.9"/><path d="M18 16.7h1"/>', 1.7],
  logo: ['<path d="M5 15.5V8.5l7 6 7-6v7"/>', 2.8],
  list: ['<path d="M4 7h16M4 12h16M4 17h10"/>', 2],
  check: ['<path d="m5 12 5 5L20 7"/>', 2]
};

function icon(parent, key, x, y, size, color, alpha) {
  const def = ICONS[key];
  const svg = '<svg xmlns="http://www.w3.org/2000/svg" width="' + size + '" height="' + size + '" viewBox="0 0 24 24" fill="none" stroke="' +
    color + '" stroke-width="' + def[1] + '" stroke-linecap="round" stroke-linejoin="round" stroke-opacity="' + (alpha == null ? 1 : alpha) + '">' + def[0] + '</svg>';
  const n = figma.createNodeFromSvg(svg);
  n.name = 'icon-' + key;
  parent.appendChild(n);
  n.x = x;
  n.y = y;
  return n;
}

function pill(parent, label, o) {
  const padX = o.padX == null ? 12 : o.padX;
  const padY = o.padY == null ? 5 : o.padY;
  const f = frameNode(parent, {
    name: o.name || ('pill ' + label), x: o.x || 0, y: o.y || 0, w: 20, h: 20,
    fill: o.fill, alpha: o.alpha, stroke: o.stroke, strokeAlpha: o.strokeAlpha,
    radius: o.radius == null ? 100 : o.radius
  });
  const t = makeText(f, { str: label, size: o.size || 13, weight: o.weight, color: o.color || C.t2, ls: o.ls });
  f.resize(t.width + padX * 2, t.height + padY * 2);
  t.x = padX;
  t.y = padY;
  return f;
}

/* ---------------- 背景层 ---------------- */

function background(page, x, y) {
  const bg = frameNode(page, { name: '背景 Background', x: x, y: y, w: W, h: H, fill: C.bg });
  const glowBlue = figma.createEllipse();
  glowBlue.name = '光晕·冷蓝';
  glowBlue.resize(1900, 900);
  glowBlue.x = 1267 - 950;
  glowBlue.y = -450;
  glowBlue.fills = [{
    type: 'GRADIENT_RADIAL',
    gradientTransform: [[1, 0, 0], [0, 1, 0]],
    gradientStops: [
      { position: 0, color: { r: 64 / 255, g: 104 / 255, b: 1, a: 0.30 } },
      { position: 0.55, color: { r: 64 / 255, g: 104 / 255, b: 1, a: 0.10 } },
      { position: 1, color: { r: 64 / 255, g: 104 / 255, b: 1, a: 0 } }
    ]
  }];
  bg.appendChild(glowBlue);

  const glowViolet = figma.createEllipse();
  glowViolet.name = '光晕·紫';
  glowViolet.resize(1500, 1100);
  glowViolet.x = W - 750;
  glowViolet.y = Math.round(H * 0.26) - 550;
  glowViolet.fills = [{
    type: 'GRADIENT_RADIAL',
    gradientTransform: [[1, 0, 0], [0, 1, 0]],
    gradientStops: [
      { position: 0, color: { r: 146 / 255, g: 88 / 255, b: 1, a: 0.22 } },
      { position: 0.6, color: { r: 146 / 255, g: 88 / 255, b: 1, a: 0.07 } },
      { position: 1, color: { r: 146 / 255, g: 88 / 255, b: 1, a: 0 } }
    ]
  }];
  bg.appendChild(glowViolet);

  const arc = figma.createNodeFromSvg('<svg xmlns="http://www.w3.org/2000/svg" width="2400" height="420" viewBox="0 0 2400 420" fill="none"><path d="M0 420 Q1200 -140 2400 420" stroke="#A8C4FF" stroke-opacity="0.2" stroke-width="1.4"/></svg>');
  arc.name = '顶部弧光';
  bg.appendChild(arc);
  arc.x = -384;
  arc.y = -300;
  arc.rotation = -9;
  return bg;
}

/* ---------------- 侧栏 ---------------- */

const MENU = [
  { label: '新建任务', icon: 'plus', on: true },
  { label: '助理', icon: 'robot', tag: 'BETA' },
  { label: '专家 · 技能 · 连接器', icon: 'star' },
  { label: '定时任务', icon: 'clock' },
  { label: '资料库', icon: 'book' },
  { label: '更多', icon: 'grid', chev: true }
];

function buildSidebar(page, x) {
  const sb = frameNode(page, { name: '侧栏 Sidebar', x: x, y: 0, w: SB, h: H, fill: C.panel, alpha: 0.94 });
  const edge = frameNode(sb, { name: '右边线', x: SB - 1, y: 0, w: 1, h: H, fill: C.white, alpha: 0.07, clip: false });

  /* 顶部品牌 */
  const top = frameNode(sb, { name: '品牌区', x: 0, y: 0, w: SB, h: 62, clip: false });
  const ring = figma.createEllipse();
  ring.name = 'logo 外环';
  ring.resize(26, 26);
  ring.fills = [];
  ring.strokes = [solid(C.white, 0.82)];
  ring.strokeWeight = 1.5;
  ring.x = 18; ring.y = 18;
  top.appendChild(ring);
  const ringIn = figma.createEllipse();
  ringIn.name = 'logo 内环';
  ringIn.resize(14, 14);
  ringIn.fills = [];
  ringIn.strokes = [solid(C.white, 0.82)];
  ringIn.strokeWeight = 1.5;
  ringIn.x = 24; ringIn.y = 24;
  top.appendChild(ringIn);
  makeText(top, { str: 'AGENT', x: 53, y: 15, size: 13.5, weight: 'bold', color: C.t1, ls: 10 });
  makeText(top, { str: 'v2.0.0', x: 53, y: 33, size: 11, color: '#6F7D97', ls: 6 });
  const collapseBtn = frameNode(top, { name: '按钮·收起侧栏', x: 238, y: 17, w: 28, h: 28, radius: 8 });
  icon(collapseBtn, 'chevLeft', 6.5, 6.5, 15, '#8B96AB');

  /* 搜索 */
  const search = frameNode(sb, { name: '搜索框', x: 12, y: 62, w: 254, h: 38, radius: 12, fill: C.white, alpha: 0.05 });
  icon(search, 'search', 10, 12.5, 13, '#8B96AB');
  makeText(search, { str: '搜索任务或消息', x: 31, y: 7.5, size: 14, color: C.w60 });
  makeText(search, { str: 'Ctrl K', x: 0, y: 9.5, size: 11, color: C.w60, width: 254 - 10, align: 'RIGHT' });

  /* 发现应用 */
  const discover = frameNode(sb, { name: '发现应用', x: 12, y: 109, w: 254, h: 41, radius: 12, fill: C.white, alpha: 0.05 });
  icon(discover, 'compass', 12, 14, 13, C.t2);
  makeText(discover, { str: '发现应用', x: 32, y: 8, size: 14, color: C.t2 });
  icon(discover, 'chevDown', 230, 14, 12, C.t2, 0.7);

  /* 菜单 */
  const menu = frameNode(sb, { name: '菜单 Menu', x: 10, y: 160, w: 258, h: 251, clip: false });
  let my = 0;
  MENU.forEach(function (m) {
    const item = frameNode(menu, {
      name: '菜单·' + m.label, x: 0, y: my, w: 258, h: 41, radius: 11,
      fill: m.on ? C.white : null, alpha: m.on ? 0.11 : 1
    });
    icon(item, m.icon, 11, 13, 15, m.on ? '#CFE0FF' : C.t2, 0.85);
    makeText(item, { str: m.label, x: 36, y: 8.5, size: 14, color: m.on ? C.white : C.t2 });
    if (m.tag) {
      const tag = frameNode(item, { name: '标签 ' + m.tag, x: 0, y: 11, w: 40, h: 19, radius: 5, fill: '#8C78FF', alpha: 0.16, stroke: '#A08CFF', strokeAlpha: 0.34 });
      const tt = makeText(tag, { str: m.tag, size: 11, color: '#C3B6FF', ls: 4 });
      tag.resize(tt.width + 12, tt.height + 2);
      tt.x = 6; tt.y = 1;
      tag.x = 257 - 11 - tag.width;
    }
    if (m.chev) icon(item, 'chevDown', 235, 15, 12, C.t2, 0.7);
    my += 42;
  });

  /* 任务分组 */
  const grpTask = frameNode(sb, { name: '分组·任务', x: 0, y: 415, w: SB, h: 47, clip: false });
  makeText(grpTask, { str: '任务', x: 20, y: 18, size: 13, weight: 'bold', color: '#8B96AB' });
  makeText(grpTask, { str: '(0)', x: 0, y: 19.5, size: 12, color: C.w50, width: SB - 20, align: 'RIGHT' });
  makeText(sb, { str: '暂无任务', x: 22, y: 466, size: 13, color: C.w60 });
  const moreLink = frameNode(sb, { name: '链接·查看更多', x: 10, y: 496, w: 258, h: 35, radius: 8 });
  makeText(moreLink, { str: '查看更多', x: 12, y: 7, size: 13, color: '#8B96AB' });

  /* 空间分组 */
  const grpSpace = frameNode(sb, { name: '分组·空间', x: 0, y: 535, w: SB, h: 47, clip: false });
  makeText(grpSpace, { str: '空间', x: 20, y: 18, size: 13, weight: 'bold', color: '#8B96AB' });
  makeText(grpSpace, { str: '(0)', x: 0, y: 19.5, size: 12, color: C.w50, width: SB - 20, align: 'RIGHT' });
  const addBtn = frameNode(grpSpace, { name: '按钮·新建空间', x: 238, y: 18, w: 20, h: 20, radius: 6 });
  icon(addBtn, 'plus', 3.5, 3.5, 13, '#8B96AB');
  makeText(sb, { str: '暂无空间', x: 22, y: 586, size: 13, color: C.w60 });

  /* 底部用户区 */
  const foot = frameNode(sb, { name: '用户区', x: 0, y: H - 62, w: SB, h: 62, clip: false });
  frameNode(foot, { name: '分隔线', x: 0, y: 0, w: SB, h: 1, fill: C.white, alpha: 0.07 });
  const ava = frameNode(foot, { name: '头像', x: 20, y: 17, w: 28, h: 28, radius: 14, fill: C.violet, alpha: 0.85 });
  makeText(ava, { str: 'A', size: 12, weight: 'bold', color: C.white, centerX: 14, y: 5 });
  ava.x = 20;
  makeText(foot, { str: '本地工作区', x: 58, y: 11, size: 13.5, color: C.t1 });
  makeText(foot, { str: 'FREE PLAN', x: 58, y: 32, size: 11, color: C.w60, ls: 6 });
  const gearBtn = frameNode(foot, { name: '按钮·设置', x: 238, y: 17, w: 28, h: 28, radius: 8 });
  icon(gearBtn, 'gear', 6.5, 6.5, 15, '#8B96AB');

  return sb;
}

/* ---------------- 顶栏 / 状态栏 ---------------- */

function buildTopbar(page, x, title) {
  const bar = frameNode(page, { name: '顶栏 Topbar', x: x, y: 0, w: W - SB, h: TOP, clip: false });
  makeText(bar, { str: title, x: 26, y: 20, size: 14, color: C.t3 });
  const status = pill(bar, '在线', { size: 13, color: C.t2, fill: C.white, alpha: 0.04, stroke: C.white, strokeAlpha: 0.10 });
  status.name = '状态·在线';
  const dot = frameNode(status, { name: '绿点', x: 12, y: status.height / 2 - 2.5, w: 5, h: 5, radius: 3, fill: C.green, clip: false });
  dot.effects = [{ type: 'DROP_SHADOW', color: { r: 99 / 255, g: 230 / 255, b: 168 / 255, a: 0.8 }, offset: { x: 0, y: 0 }, radius: 8, spread: 0, visible: true, blendMode: 'NORMAL' }];
  const txt = status.findOne(function (n) { return n.type === 'TEXT'; });
  if (txt) {
    status.resize(txt.width + 36, status.height);
    txt.x = 24;
  }
  status.x = W - SB - 26 - status.width;
  status.y = Math.round((TOP - status.height) / 2);
  return bar;
}

function buildStatusbar(page, x) {
  const bar = frameNode(page, { name: '状态栏 Statusbar', x: x, y: H - BOT, w: W - SB, h: BOT, fill: C.bar, alpha: 0.94 });
  frameNode(bar, { name: '分隔线', x: 0, y: 0, w: W - SB, h: 1, fill: C.white, alpha: 0.07 });
  const logo = frameNode(bar, { name: '运行中徽标', x: 14, y: 9.5, w: 23, h: 23, radius: 12, fill: C.accent, alpha: 0.9 });
  icon(logo, 'logo', 6, 6, 11, '#0B0D16');
  const ava = frameNode(bar, { name: '工作区头像', x: 45, y: 9.5, w: 23, h: 23, radius: 12, fill: C.white, alpha: 0.06, stroke: C.white, strokeAlpha: 0.14 });
  makeText(ava, { str: 'A', size: 11, color: C.t2, centerX: 11, y: 3 });
  const bell = frameNode(bar, { name: '按钮·通知', x: 0, y: 6, w: 30, h: 30, radius: 8 });
  icon(bell, 'bell', 7.5, 7.5, 15, '#9AA6BB');
  const dev = frameNode(bar, { name: '按钮·设备', x: 0, y: 6, w: 30, h: 30, radius: 8 });
  icon(dev, 'devices', 7.5, 7.5, 15, '#9AA6BB');
  dev.x = W - SB - 12 - 30;
  bell.x = dev.x - 6 - 30;
  return bar;
}

/* ---------------- Hero / 输入区 ---------------- */

const CENTER_X = 859; // 侧栏内侧区域的中轴

function buildHero(page) {
  const hero = frameNode(page, { name: 'Hero 标题区', x: SB, y: 0, w: W - SB, h: H, clip: false });
  const cx = (W - SB) / 2;
  makeText(hero, { str: 'AGENT RUNTIME · v2.0', size: 12.5, color: '#9FB0CC', ls: 14, centerX: cx, y: 224 });
  makeText(hero, { str: '今天想让 Agent 做点什么？', size: 54, weight: 'bold', color: C.white, ls: -2, lh: 65, width: 900, align: 'CENTER', x: cx - 450, y: 266 });
  makeText(hero, { str: '把一个目标丢给它，它会自己拆步骤、调工具、把活儿干完。', size: 16, color: C.dim, lh: 27, width: 620, align: 'CENTER', x: cx - 310, y: 352 });
  return hero;
}

function toolPill(parent, key, label, x, y) {
  const f = frameNode(parent, { name: '工具·' + label, x: x, y: y, w: 20, h: 30, radius: 100, fill: C.white, alpha: 0.04, stroke: C.white, strokeAlpha: 0.12 });
  const t = makeText(f, { str: label, size: 12.5, color: '#A2AEC2' });
  const padX = 12, iconW = 12, gap = 6;
  f.resize(padX * 2 + iconW + gap + t.width, 30);
  icon(f, key, padX, 9, 12, '#A2AEC2', 0.8);
  t.x = padX + iconW + gap;
  t.y = Math.round((30 - t.height) / 2);
  return f;
}

function buildComposer(page, top) {
  const c = frameNode(page, { name: '输入框 Composer', x: CENTER_X - 390, y: top, w: 780, h: 103, radius: 30, fill: C.white, alpha: 0.14 });
  const inner = frameNode(c, { name: '输入面板', x: 1, y: 1, w: 778, h: 101, radius: 29, fill: C.composer, alpha: 0.96 });
  makeText(inner, { str: '交代一件事，或者问点什么…', x: 20, y: 15, size: 16, color: '#8B96AB' });

  let px = 12;
  const py = 57;
  [['search', '联网搜索'], ['list', '读取文件'], ['check', '自动执行']].forEach(function (pair) {
    const p = toolPill(inner, pair[0], pair[1], px, py);
    px += p.width + 7;
  });

  const send = frameNode(inner, { name: '按钮·发送', x: 778 - 8 - 38, y: 56, w: 38, h: 38, radius: 19, fill: C.violet, alpha: 0.55 });
  icon(send, 'send', 10.5, 10.5, 17, C.white);
  return c;
}

const CHIPS = ['帮我把这周的行情整理成日报', '你能做什么', '写一个数据清洗脚本', '分析下这份表格的异常'];

function buildChips(page) {
  const made = [];
  let total = 0;
  CHIPS.forEach(function (s) {
    const f = pill(page, s, { size: 13.5, color: '#B6C0D2', padX: 16, padY: 9, fill: C.white, alpha: 0.035, stroke: C.white, strokeAlpha: 0.13 });
    made.push(f);
    total += f.width;
  });
  total += (made.length - 1) * 9;
  let cx = CENTER_X - total / 2;
  made.forEach(function (f) {
    f.x = Math.round(cx);
    f.y = 612;
    cx += f.width + 9;
  });
}

/* ---------------- 对话态 ---------------- */

function buildThread(page) {
  const thread = frameNode(page, { name: '对话流 Thread', x: CENTER_X - 410, y: 0, w: 820, h: H, clip: false });

  /* 用户气泡 */
  const ub = frameNode(thread, { name: '消息·用户', x: 0, y: 104, w: 20, h: 20, radius: 18, fill: C.white, alpha: 0.09, stroke: C.white, strokeAlpha: 0.08 });
  const ut = makeText(ub, { str: '帮我把这周的行情整理成日报', size: 15.5, color: C.t1 });
  ub.resize(ut.width + 32, ut.height + 24);
  ub.cornerRadius = 18;
  ub.bottomRightRadius = 6;
  ut.x = 16;
  ut.y = 12;
  ub.x = 820 - ub.width;

  /* 机器人头像 */
  const ava = frameNode(thread, { name: '头像·Agent', x: 0, y: 182, w: 30, h: 30, radius: 15, stroke: C.white, strokeAlpha: 0.72, strokeWeight: 1.5 });
  frameNode(ava, { name: '内点', x: 8, y: 8, w: 12, h: 12, radius: 6, fill: C.white, alpha: 0.72, clip: false });

  /* 工具调用标签 */
  const tc = pill(thread, '读取文件 · 本周行情表', { x: 44, y: 182, size: 12.5, color: '#A9C0E8', padX: 11, padY: 5, fill: C.white, alpha: 0.045, stroke: C.white, strokeAlpha: 0.10, radius: 9, name: '工具调用 toolcall' });

  /* 回复正文 */
  makeText(thread, {
    str: '整理好了：本周 5 个交易日里 3 只上涨、2 只下跌，波动最大的是 SOXL（+6.4%）。日报已按「行情概览 / 异动 / 持仓影响」三段生成，需要我补一段结论建议吗？',
    size: 15.5, color: '#DDE4F0', lh: 28, width: 620,
    x: 44, y: Math.round(tc.y + tc.height + 12)
  });
  return thread;
}

/* ---------------- 主流程 ---------------- */

async function main() {
  await pickFonts();

  const page = figma.createPage();
  page.name = 'AGENT · agent.html';
  figma.currentPage = page;

  /* 01 首页态 */
  const s1 = frameNode(page, { name: '01 · 首页（Hero 态）', x: 0, y: 0, w: W, h: H, fill: C.bg });
  background(s1, 0, 0);
  buildSidebar(s1, 0);
  buildTopbar(s1, SB, '新对话');
  buildStatusbar(s1, SB);
  buildHero(s1);
  buildComposer(s1, 495);
  buildChips(s1);
  makeText(s1, { str: '演示模式 · 尚未接入模型服务', size: 12, color: C.w50, ls: 6, centerX: CENTER_X, y: 840 });
  makeText(page, { str: '01 · 首页（Hero 态）　1440 × 900', x: 0, y: -34, size: 13, color: '#8B96AB' });

  /* 02 对话态 */
  const sx = W + 140;
  const s2 = frameNode(page, { name: '02 · 对话态（Chatting）', x: sx, y: 0, w: W, h: H, fill: C.bg });
  background(s2, 0, 0);
  buildSidebar(s2, 0);
  buildTopbar(s2, SB, '帮我把这周的行情整理成日报');
  buildStatusbar(s2, SB);
  buildThread(s2);
  buildComposer(s2, H - 62 - 103);
  makeText(page, { str: '02 · 对话态（Chatting）　1440 × 900', x: sx, y: -34, size: 13, color: '#8B96AB' });

  figma.viewport.scrollAndZoomIntoView([s1, s2]);
  figma.closePlugin('已生成 2 块 AGENT 画板');
}

main().catch(function (e) {
  figma.closePlugin('生成失败：' + (e && e.message ? e.message : e));
});

