export const genieJobsUrl = (url) => {
  let [_, path] = url.split('/api/v3/jobs', 2)
  return `/genie-jobs${path}`;
}
