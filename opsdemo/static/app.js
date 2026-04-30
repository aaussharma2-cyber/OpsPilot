(function () {
  const csrf = document.querySelector('meta[name="csrf-token"]')?.content;

  // ── Mobile sidebar toggle ───────────────────────────────────────────────
  const sidebarToggle  = document.getElementById('sidebar-toggle');
  const sidebarOverlay = document.getElementById('sidebar-overlay');
  const sidebar        = document.getElementById('sidebar');

  function openSidebar() {
    sidebar?.classList.add('is-open');
    sidebarOverlay?.classList.add('is-open');
    document.body.style.overflow = 'hidden';
  }
  function closeSidebar() {
    sidebar?.classList.remove('is-open');
    sidebarOverlay?.classList.remove('is-open');
    document.body.style.overflow = '';
  }

  sidebarToggle?.addEventListener('click', openSidebar);
  sidebarOverlay?.addEventListener('click', closeSidebar);
  // Close on nav link tap
  sidebar?.querySelectorAll('.sidebar-link').forEach(link =>
    link.addEventListener('click', closeSidebar)
  );

  // ── Kanban drag-and-drop ────────────────────────────────────────────────
  const cards = document.querySelectorAll('article[data-task-id]');
  const cols  = document.querySelectorAll('[data-status-column]');
  let dragged = null;

  function colBadge(col) {
    return col.querySelector('.page-head .badge');
  }

  function refreshBadge(col) {
    const b = colBadge(col);
    if (b) b.textContent = col.querySelectorAll('.task-card').length;
  }

  cards.forEach((card) => {
    card.addEventListener('dragstart', (e) => {
      if (e.target.closest('details, summary, form, input, select, textarea, button')) return;
      dragged = card;
      card.style.opacity = '0.5';
    });
    card.addEventListener('dragend', () => {
      if (dragged) dragged.style.opacity = '1';
      dragged = null;
    });
    // Click anywhere on card body (not buttons/form) opens detail panel
    card.addEventListener('click', (e) => {
      if (e.target.closest('button, form, details, summary, input, select, textarea, a')) return;
      openPanel(card.dataset.taskId);
    });
  });

  // Click on task title spans in backlog / done tables
  document.querySelectorAll('.task-card-title[data-task-id]').forEach((span) => {
    span.addEventListener('click', () => openPanel(span.dataset.taskId));
  });

  cols.forEach((col) => {
    col.addEventListener('dragover', (e) => { e.preventDefault(); col.classList.add('drag-over'); });
    col.addEventListener('dragleave', () => col.classList.remove('drag-over'));
    col.addEventListener('drop', async (e) => {
      e.preventDefault();
      col.classList.remove('drag-over');
      if (!dragged) return;

      // ── KEY FIX: capture before dragend fires and nulls `dragged` ──────
      const card    = dragged;
      dragged       = null;
      // ────────────────────────────────────────────────────────────────────

      const fromCol = card.closest('[data-status-column]');
      const taskId  = card.dataset.taskId;
      const status  = col.dataset.statusColumn;

      try {
        const res = await fetch(`/tasks/${taskId}/move`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
          body: JSON.stringify({ status }),
        });
        const payload = await res.json();
        if (!res.ok || !payload.ok) throw new Error(payload.error || 'Move failed');

        col.querySelector('.task-list').appendChild(card);
        card.dataset.status = status;
        card.style.opacity = '1';
        card.style.outline = '2px solid #10b981';
        setTimeout(() => { card.style.outline = ''; }, 700);

        // Update column badge counts
        if (fromCol) refreshBadge(fromCol);
        refreshBadge(col);
      } catch (err) {
        card.style.opacity = '1';
        alert(err.message || 'Could not move task');
      }
    });
  });

  // ── Task detail panel ───────────────────────────────────────────────────
  const panel   = document.getElementById('task-panel');
  const overlay = document.getElementById('tp-overlay');
  const tpBody  = document.getElementById('tp-body');
  const tpClose = document.getElementById('tp-close');
  const tpBadge = document.getElementById('tp-status-badge');

  function openPanel(taskId) {
    if (!panel) return;
    panel.setAttribute('aria-hidden', 'false');
    panel.classList.add('open');
    overlay.classList.add('open');
    tpBody.innerHTML = '<p class="subtle" style="padding:1rem">Loading…</p>';
    loadPanel(taskId);
  }

  function closePanel() {
    if (!panel) return;
    panel.classList.remove('open');
    overlay.classList.remove('open');
    panel.setAttribute('aria-hidden', 'true');
  }

  async function loadPanel(taskId) {
    try {
      const res  = await fetch(`/tasks/${taskId}/panel`);
      const html = await res.text();
      tpBody.innerHTML = html;

      // Update header badge with current status
      const statusSel = tpBody.querySelector('[name="status"]');
      if (statusSel && tpBadge) tpBadge.textContent = statusSel.value;

      // Wire AJAX form submit
      const form = document.getElementById('tp-form');
      if (!form) return;
      form.addEventListener('submit', async (ev) => {
        ev.preventDefault();
        const saveMsg = document.getElementById('tp-save-msg');
        const btn = form.querySelector('[type="submit"]');
        btn.disabled = true;
        try {
          const data = new FormData(form);
          const r = await fetch(form.action, {
            method: 'POST',
            headers: { 'X-Requested-With': 'XMLHttpRequest' },
            body: data,
          });
          const result = await r.json();
          if (!result.ok) throw new Error(result.error || 'Save failed');

          updateCard(result.task);
          if (tpBadge) tpBadge.textContent = result.task.status;
          if (saveMsg) { saveMsg.textContent = 'Saved'; saveMsg.style.display = ''; }
          setTimeout(() => { if (saveMsg) saveMsg.style.display = 'none'; }, 2000);
          // Reload history section
          loadPanel(taskId);
        } catch (err) {
          alert(err.message || 'Could not save task');
        } finally {
          btn.disabled = false;
        }
      });
    } catch (err) {
      tpBody.innerHTML = `<p class="subtle" style="padding:1rem">Failed to load task.</p>`;
    }
  }

  function updateCard(task) {
    const card = document.querySelector(`[data-task-id="${task.id}"]`);
    if (!card) return;

    // Title
    const h3 = card.querySelector('.task-card-title');
    if (h3) h3.textContent = task.title;

    // Priority badge (first badge in task-meta)
    const pb = card.querySelector('.task-meta .badge');
    if (pb) {
      const cls = ['Critical', 'High'].includes(task.priority) ? 'danger'
                : task.priority === 'Medium' ? 'warning' : 'info';
      pb.className = `badge ${cls}`;
      pb.textContent = task.priority;
    }

    // Overdue class
    const today = new Date().toISOString().split('T')[0];
    const overdue = task.due_date && task.due_date < today;
    card.classList.toggle('overdue', overdue);
    card.dataset.status   = task.status;
    card.dataset.priority = task.priority;
    card.dataset.due      = task.due_date || '';

    // Move card to new column if status changed
    const currentCol = card.closest('[data-status-column]');
    if (currentCol && currentCol.dataset.statusColumn !== task.status) {
      const targetCol = document.querySelector(`[data-status-column="${CSS.escape(task.status)}"]`);
      if (targetCol) {
        const fromCol = currentCol;
        targetCol.querySelector('.task-list').appendChild(card);
        refreshBadge(fromCol);
        refreshBadge(targetCol);
      }
    }
  }

  if (tpClose) tpClose.addEventListener('click', closePanel);
  if (overlay) overlay.addEventListener('click', closePanel);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closePanel(); });

  // ── Backlog multi-select ─────────────────────────────────────────────────
  const chkAll    = document.getElementById('chk-all');
  const bulkBtn   = document.getElementById('bulk-assign-btn');
  const bulkCount = document.getElementById('bulk-count');

  function syncBulk() {
    const checked = document.querySelectorAll('.backlog-chk:checked');
    const total   = document.querySelectorAll('.backlog-chk');
    if (bulkCount) bulkCount.textContent = `${checked.length} selected`;
    if (bulkBtn)   bulkBtn.disabled = checked.length === 0;
    if (chkAll) {
      chkAll.checked = total.length > 0 && checked.length === total.length;
      chkAll.indeterminate = checked.length > 0 && checked.length < total.length;
    }
  }
  document.querySelectorAll('.backlog-chk').forEach(c => c.addEventListener('change', syncBulk));
  if (chkAll) {
    chkAll.addEventListener('change', () => {
      document.querySelectorAll('.backlog-chk').forEach(c => { c.checked = chkAll.checked; });
      syncBulk();
    });
  }

  // ── Settings board column reorder ───────────────────────────────────────
  const colList = document.getElementById('col-list');
  if (colList) {
    let dragRow = null;

    colList.querySelectorAll('.col-row').forEach((row) => {
      row.setAttribute('draggable', 'true');

      row.addEventListener('dragstart', (e) => {
        if (e.target.closest('details, summary, form, input, select, button')) { e.preventDefault(); return; }
        dragRow = row;
        row.classList.add('dragging');
      });
      row.addEventListener('dragend', () => {
        row.classList.remove('dragging');
        colList.querySelectorAll('.col-row').forEach(r => r.classList.remove('drag-target'));
        dragRow = null;
        saveColumnOrder();
      });
      row.addEventListener('dragover', (e) => {
        e.preventDefault();
        if (dragRow && dragRow !== row) {
          row.classList.add('drag-target');
          const rows = [...colList.querySelectorAll('.col-row')];
          if (rows.indexOf(dragRow) < rows.indexOf(row)) row.after(dragRow);
          else row.before(dragRow);
        }
      });
      row.addEventListener('dragleave', () => row.classList.remove('drag-target'));
    });

    async function saveColumnOrder() {
      const ids = [...colList.querySelectorAll('.col-row')].map(r => r.dataset.colId);
      await fetch('/settings/board/reorder', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
        body: JSON.stringify({ ids }),
      });
    }
  }
  // ── Dashboard customize mode ─────────────────────────────────────────────
  const customizeBtn  = document.getElementById('customize-btn');
  const dashboardEl   = document.getElementById('dashboard');
  const widgetModal   = document.getElementById('widget-modal');
  const modalClose    = document.getElementById('widget-modal-close');
  const addMetricBtn  = document.getElementById('add-metric-btn');
  const addPanelBtn   = document.getElementById('add-panel-btn');
  const panelZone     = document.getElementById('panel-zone');
  const pickerGrid    = document.getElementById('widget-picker-grid');

  let customizeMode = false;
  let addingToZone  = null;

  if (customizeBtn && dashboardEl) {
    customizeBtn.addEventListener('click', () => {
      customizeMode = !customizeMode;
      dashboardEl.classList.toggle('customize-mode', customizeMode);
      customizeBtn.textContent = customizeMode ? 'Done' : 'Customize';
      document.querySelectorAll('.widget-panel').forEach(p => {
        p.setAttribute('draggable', customizeMode ? 'true' : 'false');
      });
    });
  }

  // Remove widget
  async function removeWidget(id) {
    if (!confirm('Remove this widget from the dashboard?')) return;
    const fd = new FormData();
    fd.append('csrf_token', csrf);
    const res = await fetch(`/dashboard/widgets/${id}/delete`, {
      method: 'POST',
      headers: { 'X-Requested-With': 'XMLHttpRequest', 'X-CSRF-Token': csrf },
    });
    const data = await res.json().catch(() => ({}));
    if (data.ok) {
      const el = document.querySelector(`[data-widget-id="${id}"]`);
      el?.remove();
    }
  }

  document.querySelectorAll('.widget-remove-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      removeWidget(btn.dataset.widgetId);
    });
  });

  // Widget modal
  function openWidgetModal(zone) {
    if (!widgetModal || !pickerGrid) return;
    addingToZone = zone;
    // Show only matching zone buttons
    pickerGrid.querySelectorAll('.widget-option-btn').forEach(btn => {
      btn.classList.toggle('hidden', btn.dataset.zone !== zone);
    });
    widgetModal.classList.add('open');
    widgetModal.setAttribute('aria-hidden', 'false');
  }

  function closeWidgetModal() {
    widgetModal?.classList.remove('open');
    widgetModal?.setAttribute('aria-hidden', 'true');
    addingToZone = null;
  }

  addMetricBtn?.addEventListener('click', () => openWidgetModal('metric'));
  addPanelBtn?.addEventListener('click',  () => openWidgetModal('panel'));
  modalClose?.addEventListener('click',   closeWidgetModal);
  widgetModal?.addEventListener('click', e => { if (e.target === widgetModal) closeWidgetModal(); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape' && addingToZone) closeWidgetModal(); });

  // Add widget
  pickerGrid?.querySelectorAll('.widget-option-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const fd = new FormData();
      fd.append('csrf_token', csrf);
      fd.append('widget_type', btn.dataset.widgetType);
      const res = await fetch('/dashboard/widgets/add', {
        method: 'POST',
        headers: { 'X-Requested-With': 'XMLHttpRequest', 'X-CSRF-Token': csrf },
        body: fd,
      });
      const data = await res.json().catch(() => ({}));
      if (data.ok) { closeWidgetModal(); window.location.reload(); }
    });
  });

  // Panel zone drag-to-reorder
  let draggedPanel = null;

  document.querySelectorAll('.widget-panel').forEach(panel => {
    panel.addEventListener('dragstart', e => {
      if (!customizeMode) { e.preventDefault(); return; }
      draggedPanel = panel;
      panel.style.opacity = '0.5';
    });
    panel.addEventListener('dragend', () => {
      if (draggedPanel) draggedPanel.style.opacity = '1';
      document.querySelectorAll('.widget-panel').forEach(p => p.classList.remove('drag-over'));
      draggedPanel = null;
      savePanelOrder();
    });
    panel.addEventListener('dragover', e => {
      e.preventDefault();
      if (!draggedPanel || draggedPanel === panel) return;
      panel.classList.add('drag-over');
      const panels = [...panelZone.querySelectorAll('.widget-panel')];
      if (panels.indexOf(draggedPanel) < panels.indexOf(panel)) panel.after(draggedPanel);
      else panel.before(draggedPanel);
    });
    panel.addEventListener('dragleave', () => panel.classList.remove('drag-over'));
  });

  async function savePanelOrder() {
    if (!panelZone) return;
    const ids = [...panelZone.querySelectorAll('.widget-panel')].map(p => p.dataset.widgetId);
    await fetch('/dashboard/widgets/reorder', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
      body: JSON.stringify({ ids }),
    });
  }

  document.querySelectorAll('form[data-disable-on-submit]').forEach((form) => {
    form.addEventListener('submit', () => {
      form.querySelectorAll('button[type="submit"]').forEach((button) => {
        button.disabled = true;
        button.dataset.originalText = button.textContent;
        button.textContent = 'Working...';
      });
    });
  });

  // ── Flash message auto-dismiss + close button ─────────────────────────────
  document.querySelectorAll('.flash').forEach((flash) => {
    const closeBtn = document.createElement('button');
    closeBtn.className = 'flash-close';
    closeBtn.innerHTML = '✕';
    closeBtn.setAttribute('aria-label', 'Dismiss');
    closeBtn.type = 'button';
    closeBtn.addEventListener('click', () => dismissFlash(flash));
    flash.appendChild(closeBtn);

    if (flash.dataset.autohide !== 'false') {
      setTimeout(() => dismissFlash(flash), 6000);
    }
  });

  function dismissFlash(el) {
    if (!el || !el.parentNode) return;
    el.style.opacity = '0';
    el.style.transform = 'translateY(-6px)';
    setTimeout(() => el.remove(), 300);
  }

  // ── Invoice form — renewal prefill ───────────────────────────────────────
  const renewalPrefill = document.getElementById('renewal-prefill');
  const renewalHint    = document.getElementById('renewal-hint');
  const clearPrefill   = document.getElementById('clear-renewal-prefill');

  if (renewalPrefill) {
    renewalPrefill.addEventListener('change', () => {
      const opt = renewalPrefill.options[renewalPrefill.selectedIndex];
      if (!opt.value) {
        if (renewalHint) renewalHint.style.display = 'none';
        return;
      }
      const refInput   = document.getElementById('inv-reference');
      const partyInput = document.getElementById('inv-party');
      const amtInput   = document.getElementById('inv-amount');
      const dueInput   = document.getElementById('inv-due-date');

      if (refInput   && !refInput.value)   refInput.value   = opt.dataset.ref   || '';
      if (partyInput && !partyInput.value) partyInput.value = opt.dataset.party || '';
      if (amtInput)   amtInput.value   = parseFloat(opt.dataset.amount || 0).toFixed(2);
      if (dueInput)   dueInput.value   = opt.dataset.due || '';

      if (renewalHint) renewalHint.style.display = '';
    });

    if (clearPrefill) {
      clearPrefill.addEventListener('click', () => {
        renewalPrefill.value = '';
        if (renewalHint) renewalHint.style.display = 'none';
        ['inv-reference','inv-party','inv-amount','inv-due-date'].forEach(id => {
          const el = document.getElementById(id);
          if (el) el.value = id === 'inv-amount' ? '0' : '';
        });
      });
    }
  }

  // ── Send Invoice Modal ────────────────────────────────────────────────────
  const invoiceModal = document.getElementById('invoice-modal');
  const sendInvoiceForm = document.getElementById('send-invoice-form');

  if (invoiceModal && sendInvoiceForm) {
    document.querySelectorAll('.open-invoice-modal').forEach((btn) => {
      btn.addEventListener('click', () => {
        const { renewalId, renewalTitle, renewalCost, renewalDate, contactEmail, contactName } = btn.dataset;
        sendInvoiceForm.action = `/renewals/${renewalId}/send_invoice`;
        document.getElementById('modal-renewal-title').textContent = renewalTitle || '—';
        document.getElementById('modal-renewal-amount').textContent = '$' + parseFloat(renewalCost || 0).toFixed(2);
        const parts = (renewalDate || '').split('-');
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        document.getElementById('modal-renewal-date').textContent =
          parts.length === 3 ? `${parseInt(parts[2])} ${months[parseInt(parts[1]) - 1]} ${parts[0]}` : renewalDate;
        document.getElementById('modal-email').value = contactEmail || '';
        document.getElementById('modal-name').value = contactName || '';
        openModal(invoiceModal);
        setTimeout(() => document.getElementById('modal-email').focus(), 80);
      });
    });

    document.querySelectorAll('.modal-close-invoice').forEach((btn) => {
      btn.addEventListener('click', () => closeModal(invoiceModal));
    });
    invoiceModal.addEventListener('click', (e) => { if (e.target === invoiceModal) closeModal(invoiceModal); });
  }

  // ── Edit Renewal Modal ────────────────────────────────────────────────────
  const editModal = document.getElementById('edit-modal');
  const editRenewalForm = document.getElementById('edit-renewal-form');

  if (editModal && editRenewalForm) {
    document.querySelectorAll('.open-edit-modal').forEach((btn) => {
      btn.addEventListener('click', () => {
        const d = btn.dataset;
        editRenewalForm.action = `/renewals/${d.renewalId}/edit`;
        document.getElementById('edit-title').value = d.title || '';
        document.getElementById('edit-category').value = d.category || '';
        document.getElementById('edit-provider').value = d.provider || '';
        document.getElementById('edit-renew-on').value = d.renewOn || '';
        document.getElementById('edit-cost').value = d.cost || '0';
        document.getElementById('edit-status').value = d.status || 'Active';
        document.getElementById('edit-auto-renew').checked = d.autoRenew === '1';
        document.getElementById('edit-contact-name').value = d.contactName || '';
        document.getElementById('edit-contact-email').value = d.contactEmail || '';
        document.getElementById('edit-notes').value = d.notes || '';
        openModal(editModal);
        setTimeout(() => document.getElementById('edit-title').focus(), 80);
      });
    });

    document.querySelectorAll('.modal-close-edit').forEach((btn) => {
      btn.addEventListener('click', () => closeModal(editModal));
    });
    editModal.addEventListener('click', (e) => { if (e.target === editModal) closeModal(editModal); });
  }

  // ── Shared modal helpers ──────────────────────────────────────────────────
  function openModal(modal) {
    modal.removeAttribute('hidden');
    modal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';
  }

  function closeModal(modal) {
    modal.setAttribute('hidden', '');
    modal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
  }

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    [invoiceModal, editModal].forEach((m) => { if (m && !m.hidden) closeModal(m); });
  });

})();
