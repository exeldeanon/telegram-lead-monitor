/*
 * Signal Desk Mini App
 *
 * Expected API contract (all requests include Authorization/session context
 * established by the backend from validated Telegram.WebApp.initData):
 *   GET   /api/leads?filter=new|review|all&limit=20
 *         -> { leads: Lead[], stats: { new, review, sent } }
 *   GET   /api/settings -> { mode: "manual" | "approval", template: string }
 *   PATCH /api/settings { mode } -> { mode }
 *   GET   /api/chats -> { accounts: [{ name, authorized, chats: Chat[] }] }
 *   PUT   /api/chats { selections: { accountName: [chatId] } }
 *   POST  /api/leads/:id/propose { message } -> { lead: Lead }
 *   POST  /api/leads/:id/skip -> { lead: Lead }
 *   POST  /api/leads/:id/suppress -> { lead: Lead }  // permanent denylist
 *   POST  /api/leads/:id/send { message } -> { lead: Lead }
 *
 * The UI has a demo fallback so it can be previewed as a static folder. The
 * real backend must validate initData on the server; never trust initDataUnsafe.
 */

(() => {
  'use strict';

  const tg = window.Telegram?.WebApp;
  if (tg) {
    tg.ready();
    tg.expand();
    tg.setHeaderColor?.('#12110f');
    tg.setBackgroundColor?.('#f2efe7');
  }

  const API_BASE = window.MINIAPP_API_BASE || '';
  const state = {
    filter: 'new',
    mode: 'manual',
    template: 'Привет, {{first_name}}! Увидел ваше сообщение про «{{topic}}».\n\nЕсли вопрос ещё актуален, могу коротко поделиться полезным вариантом. Написать?',
    selectedLead: null,
    leads: [],
    stats: { new: 0, review: 0, sent: 0 },
    loading: false,
    chats: { accounts: [], search: '', loading: false, saving: false, open: false },
  };

  const DEMO_LEADS = [
    { id: 'demo-1', status: 'new', source: 'Бизнес Чат', source_handle: '@business_chat', name: 'Иван Иванов', username: 'ivanov', initials: 'ИИ', topic: 'купить лиды', message: 'Ребята, где можно купить лиды на РКО?', created_at: '2 мин назад', score: 92 },
    { id: 'demo-2', status: 'new', source: 'Leadgen Marketplace', source_handle: '@leadgen_market', name: 'Мария К.', username: 'maria_k', initials: 'МК', topic: 'трафик на запуск', message: 'Кто подскажет рабочий источник трафика на новый запуск?', created_at: '11 мин назад', score: 84 },
    { id: 'demo-3', status: 'review', source: 'Арбитраж / вопросы', source_handle: '@traffic_talk', name: 'Алексей Р.', username: 'alex_r', initials: 'АР', topic: 'холодные лиды', message: 'У кого ещё просела конверсия? Ищу свежий подход, старый выгорел.', created_at: '34 мин назад', score: 77 },
  ];

  const DEMO_CHAT_ACCOUNTS = [{ name: 'demo-account', authorized: true, chats: [
    { id: -1001001, title: 'Бизнес Чат', username: 'business_chat', kind: 'group', selected: true, unread_count: 4 },
    { id: -1001002, title: 'Leadgen Marketplace', username: 'leadgen_market', kind: 'group', selected: true, unread_count: 0 },
    { id: -1001003, title: 'Арбитраж / вопросы', username: 'traffic_talk', kind: 'channel', selected: false, unread_count: 12 },
  ] }];

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
  const escapeHTML = (value = '') => String(value).replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[char]));
  const initials = (lead) => escapeHTML(lead.initials || String(lead.name || '?').split(/\s+/).map((part) => part[0]).join('').slice(0, 2));

  function visibleLeads() {
    if (state.filter === 'all') return state.leads;
    return state.leads.filter((lead) => state.filter === 'review' ? lead.status === 'review' : lead.status === 'new');
  }

  function statusLabel(status) {
    return ({ new: 'Новый сигнал', review: 'На проверке', sent: 'Отправлено', skipped: 'Пропущен', suppressed: 'Не связываться' })[status] || status;
  }

  function leadCard(lead, index) {
    const isSent = lead.status === 'sent';
    const isReview = lead.status === 'review';
    const source = escapeHTML(lead.source || 'Telegram-чат');
    const handle = escapeHTML(lead.source_handle || '');
    const message = escapeHTML(lead.message || '');
    const topic = escapeHTML(lead.topic || 'сигнал');
    const actionLabel = isSent ? 'Открыть карточку' : 'Предложить отправку';
    return `<article class="lead-card" data-id="${escapeHTML(lead.id)}" data-status="${escapeHTML(lead.status)}" style="animation-delay:${index * 55}ms">
      <div class="lead-topline">
        <div class="source-line"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 5.8A2.8 2.8 0 0 1 6.8 3h10.4A2.8 2.8 0 0 1 20 5.8v6.4a2.8 2.8 0 0 1-2.8 2.8H11l-4.3 4v-4.2A2.8 2.8 0 0 1 4 12.2V5.8Z"/><path d="M8 7.5h8M8 10.5h5"/></svg><span>${source}${handle ? ` · ${handle}` : ''}</span></div>
        <span class="status-pill ${isReview ? 'review' : isSent ? 'sent' : ''}">${statusLabel(lead.status)}</span>
      </div>
      <div class="lead-person"><div class="avatar" aria-hidden="true">${initials(lead)}</div><div><p class="person-name">${escapeHTML(lead.name || 'Без имени')}</p><p class="person-handle">@${escapeHTML(lead.username || 'не указан')} · ${escapeHTML(lead.created_at || 'только что')}</p></div></div>
      <div class="signal-box"><div class="signal-meta"><span>Триггер <b class="signal-trigger">${topic}</b></span><span>${lead.score ? `match ${escapeHTML(lead.score)}%` : ''}</span></div><p class="signal-text">${message}</p></div>
      ${isSent ? '' : `<div class="lead-actions"><button class="button button-quiet" data-action="skip" type="button">Пропустить</button><button class="button button-warn" data-action="suppress" type="button">Не связываться</button><button class="button button-primary" data-action="compose" type="button">${actionLabel}</button></div>`}
    </article>`;
  }

  function render() {
    const list = $('#lead-list');
    const leads = visibleLeads();
    list.setAttribute('aria-busy', state.loading ? 'true' : 'false');
    list.innerHTML = leads.map(leadCard).join('');
    $('#empty-state').hidden = state.loading || leads.length > 0;
    $('#new-count').textContent = state.stats.new;
    $('#stat-new').textContent = state.stats.new;
    $('#stat-review').textContent = state.stats.review;
    $('#stat-sent').textContent = state.stats.sent;
    $('#mode-select').value = state.mode;
    $$('.filter-tab').forEach((tab) => {
      const active = tab.dataset.filter === state.filter;
      tab.classList.toggle('is-active', active);
      tab.setAttribute('aria-selected', String(active));
    });
  }

  function showToast(message) {
    const toast = $('#toast');
    toast.textContent = message;
    toast.classList.add('is-visible');
    clearTimeout(showToast.timer);
    showToast.timer = setTimeout(() => toast.classList.remove('is-visible'), 2600);
  }

  function openChatPicker() {
    state.chats.open = true;
    $('#chat-sheet-backdrop').hidden = false;
    $('#chat-sheet').classList.add('is-open');
    $('#chat-sheet').setAttribute('aria-hidden', 'false');
    loadChats();
  }

  function closeChatPicker() {
    state.chats.open = false;
    $('#chat-sheet').classList.remove('is-open');
    $('#chat-sheet').setAttribute('aria-hidden', 'true');
    setTimeout(() => { $('#chat-sheet-backdrop').hidden = true; }, 280);
  }

  function filteredChats(account) {
    const query = state.chats.search.trim().toLowerCase();
    if (!query) return account.chats || [];
    return (account.chats || []).filter((chat) => `${chat.title} ${chat.username} ${chat.id}`.toLowerCase().includes(query));
  }

  function renderChatPicker() {
    const list = $('#chat-account-list');
    const accounts = state.chats.accounts || [];
    $('#chat-picker-status').textContent = state.chats.loading ? 'Получаю список диалогов…' : state.chats.saving ? 'Сохраняю выбор…' : '';
    if (!accounts.length && !state.chats.loading) {
      list.innerHTML = '<div class="chat-empty"><span>⌁</span><strong>Аккаунты пока не готовы</strong><p>Проверьте String Session и обновите список.</p></div>';
      return;
    }
    list.innerHTML = accounts.map((account) => {
      const chats = filteredChats(account);
      const selected = (account.chats || []).filter((chat) => chat.selected).length;
      const body = account.authorized ? (chats.length ? chats.map((chat) => `<label class="chat-row"><input type="checkbox" data-account="${escapeHTML(account.name)}" data-chat-id="${escapeHTML(chat.id)}" ${chat.selected ? 'checked' : ''}><span class="chat-check" aria-hidden="true">✓</span><span class="chat-row-copy"><strong>${escapeHTML(chat.title)}</strong><small>${chat.username ? `@${escapeHTML(chat.username)}` : escapeHTML(chat.kind || 'group')} · ${escapeHTML(chat.id)}</small></span>${chat.unread_count ? `<b class="chat-unread">${escapeHTML(chat.unread_count)}</b>` : ''}</label>`).join('') : '<div class="chat-empty chat-empty-small">По этому фильтру ничего нет.</div>') : `<div class="chat-empty chat-empty-small"><strong>Нет авторизации</strong><p>${escapeHTML(account.error || 'Нужна String Session')}</p></div>`;
      return `<section class="chat-account" data-account-section="${escapeHTML(account.name)}"><div class="chat-account-head"><div><span class="account-kicker">АККАУНТ</span><h3>${escapeHTML(account.name)}</h3></div><span class="account-count">${selected} выбрано</span></div>${body}</section>`;
    }).join('');
  }

  async function loadChats() {
    state.chats.loading = true;
    renderChatPicker();
    try {
      const data = await request('/api/chats');
      state.chats.accounts = data?.accounts || [];
    } catch (_) {
      if (!tg?.initData && !API_BASE) state.chats.accounts = DEMO_CHAT_ACCOUNTS.map((account) => ({ ...account, chats: account.chats.map((chat) => ({ ...chat })) }));
      else showToast('Не удалось получить список чатов');
    } finally {
      state.chats.loading = false;
      renderChatPicker();
    }
  }

  async function saveChatSelection() {
    const selections = {};
    state.chats.accounts.forEach((account) => {
      selections[account.name] = (account.chats || []).filter((chat) => chat.selected).map((chat) => Number(chat.id));
    });
    $$('#chat-account-list input[type="checkbox"]').forEach((input) => {
      const account = state.chats.accounts.find((item) => item.name === input.dataset.account);
      const chat = account?.chats?.find((item) => Number(item.id) === Number(input.dataset.chatId));
      if (chat) chat.selected = input.checked;
    });
    state.chats.accounts.forEach((account) => {
      selections[account.name] = (account.chats || []).filter((chat) => chat.selected).map((chat) => Number(chat.id));
    });
    state.chats.saving = true;
    renderChatPicker();
    try {
      const data = await request('/api/chats', { method: 'PUT', body: JSON.stringify({ selections }) });
      if (data?.accounts) state.chats.accounts = data.accounts;
      closeChatPicker();
      showToast('Список чатов сохранён');
    } catch (_) {
      if (!tg?.initData && !API_BASE) {
        state.chats.accounts.forEach((account) => (account.chats || []).forEach((chat) => { chat.selected = Boolean(selections[account.name]?.includes(Number(chat.id))); }));
        closeChatPicker();
        showToast('Список чатов сохранён');
      } else showToast('Не удалось сохранить список чатов');
    } finally {
      state.chats.saving = false;
      renderChatPicker();
    }
  }

  async function request(path, options = {}) {
    if (!API_BASE && !tg?.initData) throw new Error('demo mode');
    const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
    // The backend should bind this to a validated initData session. Sending
    // raw initData is optional here; it is never treated as trusted in UI.
    if (tg?.initData) headers['X-Telegram-Init-Data'] = tg.initData;
    const response = await fetch(`${API_BASE}${path}`, { ...options, headers });
    if (!response.ok) throw new Error(`API ${response.status}`);
    return response.status === 204 ? null : response.json();
  }

  async function loadData() {
    state.loading = true;
    render();
    try {
      const [leadsData, settings] = await Promise.all([request(`/api/leads?filter=${encodeURIComponent(state.filter)}&limit=20`), request('/api/settings')]);
      if (leadsData?.leads) state.leads = leadsData.leads;
      if (leadsData?.stats) state.stats = { ...state.stats, ...leadsData.stats };
      if (settings?.mode) state.mode = settings.mode;
      if (settings?.template) state.template = settings.template;
    } catch (error) {
      // Demo data is only allowed outside Telegram. An authenticated Mini App
      // must show the API error instead of pretending that a send succeeded.
      if (!tg?.initData && !API_BASE) {
        if (!state.leads.length) state.leads = DEMO_LEADS.map((lead) => ({ ...lead }));
        state.stats = { new: state.leads.filter((lead) => lead.status === 'new').length, review: state.leads.filter((lead) => lead.status === 'review').length, sent: 0 };
      } else {
        showToast('Не удалось загрузить Inbox');
      }
    } finally {
      state.loading = false;
      render();
    }
  }

  function openComposer(lead) {
    state.selectedLead = lead;
    const sheet = $('#composer-sheet');
    $('#composer-context').innerHTML = `<strong>${escapeHTML(lead.name || 'Без имени')}</strong> · ${escapeHTML(lead.source || 'Telegram-чат')}<br>${escapeHTML(lead.message || '')}`;
    const firstName = lead.name?.split(' ')[0] || 'коллега';
    const topic = lead.topic || 'этот вопрос';
    $('#message-template').value = state.template
      .replaceAll('{{first_name}}', firstName).replaceAll('{first_name}', firstName)
      .replaceAll('{{topic}}', topic).replaceAll('{topic}', topic)
      .replaceAll('{{trigger}}', topic).replaceAll('{trigger}', topic)
      .replaceAll('{{chat_title}}', lead.source || 'чате').replaceAll('{chat_title}', lead.source || 'чате');
    $('#sheet-backdrop').hidden = false;
    sheet.classList.add('is-open');
    sheet.setAttribute('aria-hidden', 'false');
    setTimeout(() => $('#message-template').focus(), 100);
  }

  function closeComposer() {
    const sheet = $('#composer-sheet');
    sheet.classList.remove('is-open');
    sheet.setAttribute('aria-hidden', 'true');
    setTimeout(() => { $('#sheet-backdrop').hidden = true; }, 280);
    state.selectedLead = null;
  }

  async function changeLead(lead, action) {
    const endpoint = action === 'suppress' ? 'suppress' : 'skip';
    try {
      await request(`/api/leads/${encodeURIComponent(lead.id)}/${endpoint}`, { method: 'POST', body: '{}' });
    } catch (_) {
      if (tg?.initData || API_BASE) { showToast('Не удалось сохранить решение'); return; }
    }
    lead.status = action === 'suppress' ? 'suppressed' : 'skipped';
    state.stats.new = state.leads.filter((item) => item.status === 'new').length;
    state.stats.review = state.leads.filter((item) => item.status === 'review').length;
    render();
    showToast(action === 'suppress' ? 'Лид добавлен в список «не связываться»' : 'Лид убран из потока');
  }

  async function sendSelected() {
    const lead = state.selectedLead;
    if (!lead) return;
    const message = $('#message-template').value.trim();
    if (!message) { showToast('Добавьте текст сообщения'); return; }
    const sendButton = $('#send-compose');
    sendButton.disabled = true;
    try {
      await request(`/api/leads/${encodeURIComponent(lead.id)}/send`, { method: 'POST', body: JSON.stringify({ message }) });
    } catch (_) {
      sendButton.disabled = false;
      showToast('Telegram не подтвердил отправку');
      return;
    }
    lead.status = 'sent';
    state.stats.new = state.leads.filter((item) => item.status === 'new').length;
    state.stats.review = state.leads.filter((item) => item.status === 'review').length;
    state.stats.sent += 1;
    state.template = message;
    closeComposer();
    render();
    showToast('Отправлено и записано в журнал');
    sendButton.disabled = false;
  }

  async function saveMode(mode) {
    state.mode = mode;
    try { await request('/api/settings', { method: 'PATCH', body: JSON.stringify({ mode }) }); } catch (_) { /* demo fallback */ }
    showToast(mode === 'manual' ? 'Ручной режим включён' : 'Подтверждение перед отправкой включено');
  }

  $('#lead-list').addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button) return;
    const card = button.closest('.lead-card');
    const lead = state.leads.find((item) => item.id === card?.dataset.id);
    if (!lead) return;
    if (button.dataset.action === 'compose') openComposer(lead);
    if (button.dataset.action === 'skip') changeLead(lead, 'skip');
    if (button.dataset.action === 'suppress') changeLead(lead, 'suppress');
  });
  $$('.filter-tab').forEach((tab) => tab.addEventListener('click', () => { state.filter = tab.dataset.filter; loadData(); }));
  $('#mode-select').addEventListener('change', (event) => saveMode(event.target.value));
  $('#refresh-button').addEventListener('click', () => { loadData(); showToast('Обновляю поток…'); });
  $('#chats-button').addEventListener('click', openChatPicker);
  $('#close-chat-sheet').addEventListener('click', closeChatPicker);
  $('#cancel-chat-selection').addEventListener('click', closeChatPicker);
  $('#chat-sheet-backdrop').addEventListener('click', closeChatPicker);
  $('#chat-refresh').addEventListener('click', loadChats);
  $('#save-chat-selection').addEventListener('click', saveChatSelection);
  $('#chat-search').addEventListener('input', (event) => { state.chats.search = event.target.value; renderChatPicker(); });
  $('#close-sheet').addEventListener('click', closeComposer);
  $('#cancel-compose').addEventListener('click', closeComposer);
  $('#sheet-backdrop').addEventListener('click', closeComposer);
  $('#send-compose').addEventListener('click', sendSelected);
  $$('.template-hint button').forEach((button) => button.addEventListener('click', () => {
    const textarea = $('#message-template');
    const start = textarea.selectionStart;
    textarea.setRangeText(button.dataset.token, start, textarea.selectionEnd, 'end');
    textarea.focus();
  }));
  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;
    if (state.selectedLead) closeComposer();
    else if (state.chats.open) closeChatPicker();
  });

  loadData();
})();
