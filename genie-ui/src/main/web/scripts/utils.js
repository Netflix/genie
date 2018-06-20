import $ from "jquery";
import moment from "moment";

// var p = {foo: [1,2,3], bar: 42};
// setting traditional to true generates
// foo=1&foo=2&foo=3&bar=42
$.ajaxSettings.traditional = true;

export const genieJobsUrl = url => {
  const [ignored, path] = url.split("/api/v3/jobs", 2);
  return `/output${path}`;
};

export const fileUrl = url => {
  const [ignored, path] = url.split("/api/v3/jobs", 2);
  return `/file${path}`;
};

export const stripHateoasTemplateUrl = url => url.replace("{?status}", "");

export const activeClusterUrl = url => url.replace("{?status}", "?status=UP");

export const activeCommandUrl = url =>
  url.replace("{?status}", "?status=ACTIVE");

export const fetch = (
  url,
  data = null,
  type = "GET",
  headers = "application/hal+json"
) => $.ajax({ global: false, type, headers: { Accept: headers }, url, data });

export const hasChanged = (o1, o2) => {
  let changed = false;
  for (const key of Object.keys(o1)) {
    if (
      (key === "src" && o1[key] === "btn") ||
      (key !== "showDetails" && (!o2 || o1[key] !== o2[key]))
    ) {
      changed = true;
      break;
    }
  }

  if (!changed) {
    for (const key of Object.keys(o2)) {
      if (key !== "showDetails" && (!o1 || o1[key] !== o2[key])) {
        changed = true;
        break;
      }
    }
  }

  return changed;
};

export const nowUtc = () => moment().utc();

// stringify moment object
export const milliSeconds = momentObj => `${momentObj}`;

export const momentFormat = (dateStr, format = "MM/DD/YYYY, H:mm:ss") =>
  dateStr ? moment.utc(dateStr).format(format) : "";

// https://github.com/moment/moment/issues/1048
export const momentDurationFormat = (durationStr, format = ":mm:ss") =>
  Math.floor(moment.duration(durationStr).asHours()) +
  moment.utc(moment.duration(durationStr).asMilliseconds()).format(format);
