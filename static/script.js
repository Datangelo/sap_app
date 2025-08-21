let allColumns = [];

// hide everything initially
['select-columns-container',
 'group-by-container',
 'aggregation-container',
 'order-by-container',
 'column-order-container'
].forEach(id => document.getElementById(id).style.display = 'none');

// upload handler
document.getElementById('upload-form').addEventListener('submit', e => {
  e.preventDefault();
  const form = new FormData(e.target);
  document.getElementById('loading').style.display = 'block';

  fetch('/x2cf_upload_file', {
    method: 'POST',
    body: form
  })
  .then(r => r.json())
  .then(cols => {
    allColumns = cols.sort();
    const cont = document.getElementById('columns-container');
    cont.innerHTML = '';
    allColumns.forEach(c => addCheckbox(cont, 'columns', c));
    document.getElementById('loading').style.display = 'none';
    document.getElementById('select-columns-container').style.display = 'block';
  })
  .catch(err => {
    document.getElementById('loading').style.display = 'none';
    alert('Error uploading file: ' + err);
  });
});

// after columns are chosen
document.getElementById('confirm-columns').addEventListener('click', e => {
  e.preventDefault();
  const selected = Array.from(
    document.querySelectorAll('input[name="columns"]:checked')
  ).map(cb => cb.value);

  // clear sub‐sections
  ['group-by-checkboxes','aggregation-checkboxes','order_by','column_order']
    .forEach(id => document.getElementById(id).innerHTML = '');

  selected.forEach(col => {
    addCheckbox(document.getElementById('group-by-checkboxes'), 'group_by',     col);
    addCheckbox(document.getElementById('aggregation-checkboxes'), 'aggregations', `${col}:sum`, `${col} (sum)`);
    addOption(document.getElementById('order_by'), col);

    // build sortable list
    const li = document.createElement('li');
    li.textContent = col;
    li.dataset.value = col;
    document.getElementById('column_order').appendChild(li);
  });

  ['group-by-container','aggregation-container','order-by-container','column-order-container']
    .forEach(id => document.getElementById(id).style.display = 'block');

  Sortable.create(document.getElementById('column_order'), { animation: 150 });
});

// final process/download
document.getElementById('process-button').addEventListener('click', e => {
  e.preventDefault();
  const form = new URLSearchParams();

  document.querySelectorAll('input[type="checkbox"]:checked').forEach(cb => {
    form.append(cb.name, cb.value);
  });

  form.append('order_by', document.getElementById('order_by').value);

  Array.from(document.getElementById('column_order').children)
    .map(li => li.dataset.value)
    .forEach(col => form.append('column_order', col));

  document.getElementById('loading').style.display = 'block';

  // ← THIS endpoint must match @app.route('/process')
  fetch('/process', {
    method: 'POST',
    body: form
  })
  .then(r => {
    if (!r.ok) return r.json().then(j => { throw new Error(j.error || 'Unknown') });
    return r.blob();
  })
  .then(blob => {
    document.getElementById('loading').style.display = 'none';
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'grouped_data.xlsx';
    document.body.appendChild(a);
    a.click();
    a.remove();
  })
  .catch(err => {
    document.getElementById('loading').style.display = 'none';
    alert('Error processing data: ' + err);
  });
});

function addCheckbox(container, name, value, labelText = value) {
  const label = document.createElement('label');
  const cb    = document.createElement('input');
  cb.type  = 'checkbox';
  cb.name  = name;
  cb.value = value;
  label.appendChild(cb);
  label.appendChild(document.createTextNode(labelText));
  container.appendChild(label);
}

function addOption(select, value) {
  const o = document.createElement('option');
  o.value       = value;
  o.textContent = value;
  select.appendChild(o);
}
