<template>
    <div id="sider_navigator">
        <div v-for="(value, index) in items" v-bind:key="value">
            <button v-bind:class="{ sider_item_chosen: index === chosen_tab }" @click="$emit('choose_tab', index)" class="sider_item">{{value}}</button><br>
        </div>
    </div>
</template>

<script>
    export default {
        name: "sider",
        props: ['chosen_tab'],
        data() {
            return {
                items: [
                    'Upload Graph Data',
                    'Upload Workload',
                    'Storage Structure Selection',
                    'Index Selection',
                    'Query Optimization'
                ]
            }
        }
    }
</script>

<style scoped>
    .sider_item{
        margin-top: -1px;
        width: 30vw;
        max-width: 350px;
        height: 10vh;
        background: none;
        outline: none;
        border: 2px black solid;
        border-top: none;
        font-weight: bold;
        font-size: 3vh;
    }
    .sider_item:active{
        color: #1f4e79;
    }
    .sider_item_chosen{
        color: #1f4e79;
    }
</style>