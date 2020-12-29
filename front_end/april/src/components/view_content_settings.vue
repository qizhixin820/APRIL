<template>
    <div>
        <!--        <div v-for="(check_items, check_name, check_index) in checkbox_list"-->
        <!--             v-bind:key="check_name">-->
        <!--            <label class="checkbox_lable">{{check_name}}</label>-->
        <!--            <div class="check_item_group">-->
        <!--                <label v-for="choose_item in check_items" v-bind:key="choose_item"-->
        <!--                       class="check_item">-->
        <!--                    <input type="radio" :name="check_name"-->
        <!--                           @change="ratio_choose[check_index] = index"/>-->
        <!--                    {{choose_item}}-->
        <!--                </label>-->
        <!--            </div>-->
        <!--            <br>-->
        <!--        </div>-->
        <div v-for="(ratio_items, ratio_name, ratio_index) in ratio_list" v-bind:key="ratio_name">
            <label class="checkbox_lable">{{ratio_name}}</label>
            <div class="check_item_group">
                <label v-for="(choose_item, index) in ratio_items" v-bind:key="choose_item"
                       class="check_item">
                    <input type="radio" :name="ratio_name"
                           @change="ratio_choose[ratio_index] = index"/> {{choose_item}}
                </label>
            </div>
            <br>
        </div>
        <button v-if="!user_specific && (chosen_tab===2 || chosen_tab===3)"
                v-on:click="start()"
                class="general_btn">start
        </button>
    </div>
</template>

<script>
    export default {
        name: "view_content_settings",
        props: ['chosen_tab', 'user_specific','load_file'],
        data() {
            return {
                timer: null,
                checkbox_list: {
                    'Graph Data': [
                        'LUBM',
                        'WatDiv',
                    ],
                    'Workload': [
                        'benchmark1',
                        'benchmark2',
                        'queries'
                    ],
                },
                ratio_choose: [0, 0, 0, 0],
                ratio_list: {
                    'Graph Data': [
                        'LUBM',
                        'WatDiv',
                    ],
                    'Workload': [
                        'benchmark1',
                        'benchmark2',
                        'queries'
                    ]
                }

            }
        },
        watch: {
            'chosen_tab': function (tag) {
                this.ratio_list = {
                    'Graph Data': [
                        'LUBM',
                        'WatDiv',
                    ],
                    'Workload': [
                        'benchmark1',
                        'benchmark2',
                        'queries'
                    ]
                }
                switch (tag) {
                    case 3:
                        this.ratio_list['Storage'] = ['storage1', 'storage2'];
                        break;
                    case 4:
                        this.ratio_list['Storage'] = ['storage1', 'storage2'];
                        this.ratio_list['Index'] = ['indices1', 'indices2'];
                        break;
                }
            },
        },
      methods: {
        // 'aas': function (index) {
        //     console.log(this)
        // }
        begin_loading() {
         // this.load_file();

          this.timer = setInterval(() => {
            this.load_file();
          }, 5000);
        },

        stop_loading() {
          this.timer && clearInterval(this.timer);
          this.timer = null;
        },

        start() {
          //this.begin_loading();
          const formData = new FormData();
          formData.set('choose', this.ratio_choose);
          //const PATH_LOG = "D:\\Download\\dbdqn\\dbdqn\\data\\log\\";
          const PATH_LOG = "C:\\PF\\GithubProjects\\APRIL\\dbdqn\\";
          if (this.chosen_tab === 2) {
            this.axiosJSON.post("/storage/start", formData).then(result => {
              if (result.data.status === "OK") {
                console.log("before enter");
                this.load_file(PATH_LOG + "dbdqn\\data\\log\\lubm.txt");
                console.log("before exit");
                console.log(result.data.result);
                alert(result.data.message);
              } else {
                alert("fail");
              }
            })
          } else if (this.chosen_tab === 3) {
            this.axiosJSON.post("/index/start", formData).then(result => {
              if (result.data.status === "OK") {
                this.load_file(PATH_LOG + "dqn-qindex\\data\\log\\lubm.txt")
                console.log(result.data.result);
                alert(result.data.message);
              } else {
                alert("fail");
              }
            })
          }
          //this.stop_loading();
        }
      },
      beforeDestroy() {
        this.stop_loading();
      }
    }
</script>

<style scoped>
    .checkbox_lable {
        font-size: 1.5vw;
        font-weight: bold;
        display: inline-block;
        width: 13vw;
        text-align: center;
        vertical-align: top;
    }

    .check_item {
        font-size: 1.5vw;
        display: inline-block;
        width: 33%;
    }

    .check_item_group {
        width: 70%;
        display: inline-block;
        margin-bottom: 10px;

    }

    .general_btn {
        font-size: 3vh;
        font-weight: bold;
        width: 30%;
        max-width: 150px;
        height: 5vh;
        border-radius: 5px;
        border: 2px black solid;
        outline: none;
        margin-top: 2%;
        margin-left: 32.5%;
    }
</style>
