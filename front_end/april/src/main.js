import Vue from 'vue'
import index from './components/index'
import { Row, Col, Modal, Upload } from 'ant-design-vue';
import './assets/css/font.less'
import axios from "axios"

const axiosJSON = axios.create({
  baseURL: 'http://localhost:8200',
});
const axiosUpload = axios.create({
  baseURL: 'http://localhost:8200',
  headers: {'Content-Type': 'multipart/form-data'}
});



Vue.config.productionTip = false;
Vue.component(Row.name, Row);
Vue.component(Col.name, Col);
Vue.component(Modal.name, Modal);
Vue.component(Upload.name, Upload);

Vue.prototype.axiosJSON = axiosJSON;
Vue.prototype.axiosUpload = axiosUpload;


new Vue({
  render: h => h(index),
}).$mount('#app');
