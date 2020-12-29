<template>
    <div>
        <strong>{{chosen_tab === 2 ? 'Customized Storage Structure':'Customized Index'}}</strong><br>
        <a-row v-if="chosen_tab === 2">
            <a-col :span="20">
                <div class="user_select_group">
                    <div>
                        <label class="select_label">divide</label>
                        <label class="select_label">t0</label>
                        <label class="select_label">by</label>
                        <select v-model="divide_p">
                            <option v-for="(each_p, index) in all_predicates"
                                    v-bind:key="index" :value="each_p">{{each_p}}
                            </option>
                        </select>
                    </div>
                </div>
            </a-col>
            <a-col :span="4">
                <button @click="add_divide()" class="general_btn">add</button>
            </a-col>
        </a-row>
        <a-row v-if="chosen_tab === 2">
            <a-col :span="20">
                <div class="user_select_group">
                    <div>
                        <label class="select_label">merge</label>
                        <input type="text" v-model="merge_t1" @change="get_t1_cols(merge_t1)"
                               style="width: 40px">
                        <label class="select_label">and</label>
                        <input type="text" v-model="merge_t2" @change="get_t2_cols(merge_t2)"
                               style="width: 40px">
                        <label class="select_label">by</label>
                        <label class="select_label">{{this.merge_t1}}.</label>
                        <select v-model="merge_c1" @change="init_merge_c1()">
                            <option v-for="(each_c, index) in merge_t1_cols"
                                    v-bind:key="index" :value="each_c">{{each_c}}
                            </option>
                        </select>
                        <label class="select_label">=</label>
                        <label class="select_label">{{this.merge_t2}}.</label>
                        <select v-model="merge_c2" @change="init_merge_c2()">
                            <option v-for="(each_c, index) in merge_t2_cols"
                                    v-bind:key="index" :value="each_c">{{each_c}}
                            </option>
                        </select>
                    </div>
                </div>
            </a-col>
            <a-col :span="4">
                <button @click="add_merge()" class="general_btn">add</button>
            </a-col>
        </a-row>
        <a-row v-if="chosen_tab === 3">
            <a-col :span="20">
                <div class="user_select_group">
                    <div>
                        <label class="select_label">table</label>
                        <input type="text" v-model="index_t" @change="init_index_cols()"
                               style="width: 40px">
                        <label class="select_label">column</label>
                        <select v-model="index_col" >
                            <option v-for="(each_col, index) in cols_on_table"
                                    v-bind:key="index" :value="each_col">{{each_col}}
                            </option>
                        </select>
                        <label class="select_label">index</label>
                        <select v-model="index_type">
                            <option>hash index</option>
                            <option>Btree index</option>
                        </select>
                    </div>
                </div>
            </a-col>
            <a-col :span="4">
                <button @click="add_index()" class="general_btn">add</button>
            </a-col>
        </a-row>

        <a-row type="flex" align="middle">
            <a-col :span="20">
                <select multiple id="user_specific_area" v-model="checkbox_selected">
                    <option v-for="(item, index) in user_select" v-bind:key="index" :value="index">
                        {{item}}
                    </option>
                </select>
            </a-col>
            <a-col :span="4">
                <button @click="remove_item" class="general_btn">remove</button>
            </a-col>
        </a-row>

        <div id="submission_bar">
            <a-row>
                <a-col :offset="6" :span="4">
                    <button id="start_btn" class="general_btn" @click="start()">start</button>
                </a-col>
                <a-col :offset="6" :span="4">
                    <label id="total_time_box"><strong>Total Time</strong></label>
                </a-col>
                <a-col :span="4">
                    <input v-model="total_time" v-bind:key="total_time" style="width: 100%; background:
                    white"
                           disabled>
                </a-col>
            </a-row>
        </div>

        <a-row style="margin-top: 10px" :gutter="{ xs: 10, sm: 20, md: 40, lg: 80}">
            <a-col :span="8">
                <button class="general_btn">save</button>
            </a-col>
            <a-col :span="8" :offset="0">
                <button class="general_btn">save & apply</button>
            </a-col>
            <a-col :span="8" :offset="0">
                <button @click="$emit('close')" class="general_btn">return</button>
            </a-col>
        </a-row>
    </div>
</template>

<script>
    export default {
        name: "user_specific",
        props: ['chosen_tab'],
        data() {
            return {
                user_select: [],
                checkbox_selected: [],
                total_time: "",
                divide_p: "",
                all_predicates: [],
                merge_t1: "",
                merge_t2: "",
                merge_t1_cols: [],
                merge_t2_cols: [],
                merge_c1: "",
                merge_c2: "",
                index_t: "",
                index_col: "",
                cols_on_table: [],
                index_type: ""
            }
        },
        created:function() {
            this.init_all_predicates();
        },
        methods: {
            init_all_predicates() {
                this.axiosJSON.get("/dao/get_all_predicates").then(result => {
                    if (result.data.status === "OK") {
                        this.all_predicates = result.data.list;
                    } else {
                        alert(result.data.message);
                    }
                })
            },
            get_t1_cols(t1) {
                this.merge_t1_cols = [];
                this.axiosJSON.get("/dao/get_cols", {
                    params: {
                        t: t1
                    }
                }).then(result => {
                    console.log(result);
                    if (result.data.status === "OK") {
                        this.merge_t1_cols = result.data.list;
                        console.log(this.merge_t1_cols)
                    } else {
                        alert(result.data.message);
                    }
                })
            },
            get_t2_cols(t2) {
                this.merge_t2_cols = [];
                this.axiosJSON.get("/dao/get_cols", {
                    params: {
                        t: t2
                    }
                }).then(result => {
                    if (result.data.status === "OK") {
                        this.merge_t2_cols = result.data.list;
                    } else {
                        alert(result.data.message);
                    }
                })
            },
            add_divide() {
                let str = "divide t0 by " + this.divide_p;
                this.user_select.push(str);
            },
            add_merge() {
                let str = "merge " + this.merge_t1 + " and " + this.merge_t2 + " by " +
                    this.merge_t1 + "." + this.merge_c1 + " = " + this.merge_t2 + "." +
                    this.merge_c2;
                this.user_select.push(str);
            },
            add_index(){
               let str = "create hash index on " + this.index_col + " on " + this.index_t;
               this.user_select.push(str);
            },
            init_merge_c1() {
                this.axiosJSON.get("/dao/get_columns", this.merge_t1).then(result => {
                    if (result.data.status === "OK") {
                        this.merge_t1_cols = result.data.list;
                    } else {
                        alert(result.data.message);
                    }
                })
            },
            init_merge_c2() {
                this.axiosJSON.get("/dao/get_columns", this.merge_t2).then(result => {
                    if (result.data.status === "OK") {
                        this.merge_t2_cols = result.data.list;
                    } else {
                        alert(result.data.message);
                    }
                })
            },
            init_index_cols() {
                console.log(this.index_t);
                this.axiosJSON.get("/dao/get_cols", {
                    params: {
                        t: this.index_t
                    }
                }).then(result => {
                    if (result.data.status === "OK") {
                        this.cols_on_table = result.data.list;
                        console.log(this.cols_on_table);
                    } else {
                        alert(result.data.message);
                    }
                })
            },
            'remove_item': function () {
                let checkbox_selected_copy = this.checkbox_selected.slice(0).reverse();
                for (let index in checkbox_selected_copy) {
                    this.user_select.splice(checkbox_selected_copy[index], 1)
                }
                this.checkbox_selected = []
            },
            start() {
                let encodeUserSelect = new Array();
                for(var i=0, len=this.user_select.length; i<len; i++) {
                    console.log(this.user_select[i]);
                    encodeUserSelect.push(this.user_select[i]);
                }
                let params = new FormData();
                params.append("user_select", JSON.stringify(encodeUserSelect));

                if (this.chosen_tab === 2) {
                    console.log(encodeUserSelect);
                    params.append("type", "lubm");
                    this.axiosJSON.post("/storage/start_user_specific", params).then(result => {
                        if (result.data.status === "OK") {
                            console.log("show results");
                            this.total_time = result.data.result;
                        } else {
                            alert(result.data.message);
                        }
                    })
                } else if (this.chosen_tab === 3) {
                    this.axiosJSON.post("/index/start_user_specific", params).then(result => {
                        if (result.data.status === "OK") {
                            console.log("show results");
                            this.total_time = result.data.result;
                        } else {
                            alert(result.data.message);
                        }
                    })
                }
            }
        },
        watch: {
            chosen_tab: function (chosen_tab) {
                this.user_select = [];
                switch (chosen_tab) {
                    case 2:
                        break;
                    case 3:
                        break;
                }
            }
        }
    }
</script>

<style scoped>
    .user_select_group {
        margin-top: 10px;
        width: 100%;
        z-index: 2;
    }

    .general_btn {
        font-size: 3vh;
        font-weight: bold;
        width: 100%;
        height: 5vh;
        border-radius: 5px;
        border: 2px black solid;
        outline: none;
        margin: 2%;
    }

    #user_specific_area {
        /*TODO*/
        margin-top: 10px;
        float: left;
        width: 95%;
        min-height: 20vh;
        background: white;
        border: 1px black solid;
    }

    .user_specific_chosen {
        margin: 3px;
        border: 1px #66ccff solid;
        border-radius: 10px;
    }

    #submission_bar {
    }

    #total_time_box {
        margin: 10px;
    }

    #start_btn {
        width: 100%;
    }

    .select_label {
        margin: 0 3%;
    }
</style>
