import $ from 'jquery';
import moment from 'moment';

export const genieJobsUrl = (url) => {
  let [_, path] = url.split('/api/v3/jobs', 2)
  return `/output${path}`;
}

export const fileUrl = (url) => {
  let [_, path] = url.split('/api/v3/jobs', 2)
    return `/file${path}`;
}

export const fetch = (url, data=null, type='GET', headers='application/hal+json') => {
  return $.ajax({
    global: false,
    type: type,
    headers: {
      'Accept': headers
    },
    url: url,
    data:data,
  });
}

export const hasChanged = (o1, o2) => {
  let changed = false;
  for (const key of Object.keys(o1)) {
    if (key !== 'showDetails' && (!o2 || o1[key] !== o2[key])) {
      changed = true;
      break;
    }
  }
  for (const key of Object.keys(o2)) {
    if (key !== 'showDetails' && (!o1 || o1[key] !== o2[key])) {
      changed = true;
      break;
    }
  }
  return changed
}

export const momentFormat = (dateStr, format='MM/DD/YYYY, h:mm:ss') =>
  moment(dateStr).format(format)

