import $ from 'jquery';
import moment from 'moment';

export const genieJobsUrl = (url) => {
  const [ignored, path] = url.split('/api/v3/jobs', 2);
  return `/output${path}`;
};

export const fileUrl = (url) => {
  const [ignored, path] = url.split('/api/v3/jobs', 2);
  return `/file${path}`;
};

export const fetch = (url, data = null, type = 'GET', headers = 'application/hal+json') => {
  return $.ajax({
    global: false,
    type,
    headers: {
      Accept: headers,
    },
    url,
    data,
  });
};

export const hasChanged = (o1, o2) => {
  let changed = false;
  // Clean up
  for (const key of Object.keys(o1)) {
    if ((key === 'src' && o1[key] === 'btn') ||
        (key !== 'showDetails' && (!o2 || o1[key] !== o2[key]))) {
      changed = true;
      break;
    }
  }

  if (!changed) {
    for (const key of Object.keys(o2)) {
      if (key !== 'showDetails' && (!o1 || o1[key] !== o2[key])) {
        changed = true;
        break;
      }
    }
  }

  return changed;
};

export const momentFormat = (dateStr, format = 'MM/DD/YYYY, h:mm:ss') =>
  moment(dateStr).format(format);

