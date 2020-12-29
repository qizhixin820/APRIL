<template>
    <div>
        <label v-if="chosen_tab === 2 || chosen_tab === 3">
            <strong>Selection Results</strong><br>
            <div style="text-align: center">
                <textarea v-model="result_text" disabled rows="5" class="result_show_box"/>
            </div>
        </label>

        <div v-if="chosen_tab === 4">
            <a-row type="flex" align="middle">
                <a-col :span="11">
                    <a-row>
                        <strong>Query Input</strong>
                    </a-row>
                    <a-row>
                        <textarea v-model="query_input" class="input_like_transfer"/>
                    </a-row>
                </a-col>
                <a-col :span="2"><div style="text-align:center; font-size: 50px">→</div></a-col>
                <a-col :span="11">
                    <a-row>
                        <strong>Query Plan Results</strong>
                    </a-row>
                    <a-row>
                        <textarea v-model="query_plan_results" class="input_like_transfer" disabled/>
                    </a-row>
                </a-col>
            </a-row>
        </div>
        <div>
            <a-row :gutter="40">
                <a-col :span="6" v-if="chosen_tab === 4"><button class="general_btn" @click="query_optimization_start1()">automatic mode</button></a-col>
                <a-col :span="6" v-if="chosen_tab === 4"><button class="general_btn" @click="query_optimization_start2()">rule-based mode</button></a-col>
                <a-col :span="3" v-else></a-col>
                <a-col :span="6"><button class="general_btn">save</button></a-col>
                <a-col :span="6"><button class="general_btn" @click="save_apply()">save & apply</button></a-col>
                <a-col :span="6" v-if="chosen_tab !== 4">
                    <button @click="chosen_tab!==4 && $emit('update:user_spcecific_opening', true)" class="general_btn">
                        {{chosen_tab === 4 ? 'rule-based mode' : 'customized mode'}}
                    </button>
                </a-col>
            </a-row>
        </div>
    </div>
</template>

<script>
    export default {
        name: "result_box",
        props: ['chosen_tab', 'file_text'],
        data() {
            return {
                result_text: "",
                /*
                query_input: 'SELECT ?X ?Y ?Z WHERE{\n' +
                    '?X rdf:type ub:GraduateStudent.\n' +
                    '?Y rdf:type ub:University.\n' +
                    '?Z rdf:type ub:Department.\n' +
                    '?X ub:memberOf ?Z.\n' +
                    '?Z ub:subOrganizationOf ?Y.\n' +
                    '?X ub:undergraduateDegreeFrom ?Y.\n' +
                    '}\n',
                query_plan_results: '?Y rdf:type ub:University.\n' +
                    '?Z ub:subOrganizationOf ?Y.\n' +
                    '?X ub:undergraduateDegreeFrom ?Y.\n' +
                    '?Z rdf:type ub:Department.\n' +
                    '?X rdf:type ub:GraduateStudent.\n' +
                    '?X ub:memberOf ?Z.\n' +
                    'Query time: 155ms'
                */
                query_input: "# Query1\r\n" +
                    "# This query bears large input and high selectivity. It queries about just one class and\r\n" +
                    "# one property and does not assume any hierarchy information or inference.\r\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" +
                    "PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>\r\n" +
                    "SELECT ?X	\r\n" +
                    "WHERE\r\n" +
                    "{\r\n" +
                    "    ?X rdf:type ub:GraduateStudent .\r\n" +
                    "    ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>\r\n" +
                    "}",
                query_plan_results: ''
            }
        },
        methods: {
            save_apply() {
                console.log(this.chosen_tab);
                if (this.chosen_tab === 2) {
                    this.axiosJSON.get("/storage/save_apply").then(result => {
                        if (result.data.status === "OK") {
                            alert(result.data.message);
                        } else {
                            alert(result.data.message);
                        }
                    })
                }
            },
            query_optimization_start0(mode) {
                console.log("starting query_optimization with mode " + mode.toString());
                console.log(this.query_input);
                console.log(this.query_plan_results);

                // input [mode, this.query_input] to something, and save the output to this.query_plan_results
                // this.query_plan_results =

                const formData = new FormData();
                formData.set('query', this.query_input);
                formData.set('mode', mode.toString());
                this.axiosJSON.post("/storage/query_optimization", formData).then(result => {
                    if (result.data.status === "OK") {
                        console.log(result.data.result);
                        this.query_plan_results = result.data.result;
                    } else {
                        alert("fail");
                    }
                });
            },
            query_optimization_start1() {
                this.query_optimization_start0(1);
            },
            query_optimization_start2() {
                this.query_optimization_start0(2);
            }
        },
        watch: {
            chosen_tab: function (value) {
                switch (value) {
                    case 2:
                        break;
                    case 3:
                        break;
                }
            },
            's': function (value) {
                console.log(value)
            },
            file_text: function(value) {
                console.log("change here");
                this.result_text = value;
            }

        }
    }
</script>

<style scoped>
    .result_show_box{
        font-size: 3vh;
        width: 95%;
        resize: none;
        background: white;
    }
    .general_btn{
        font-size: 3vh;
        font-weight: bold;
        width: 100%;
        height: 5vh;
        border-radius: 5px;
        border: 2px black solid;
        outline: none;
        margin: 2%;

    }
    .input_like_transfer{
        background: white;
        resize: none;
        height: 30vh;
        width: 100%;
    }
</style>
