import { jsonServerRestClient } from 'admin-on-rest';

// const restClient = simpleRestClient('http://localhost:3000');
const restClient = jsonServerRestClient('http://3.13.64.3:8080/api/df');
// export restClient
export default (type, resource, params) => new Promise(resolve => setTimeout(() => resolve(restClient(type, resource, params)), 500));