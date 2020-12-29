<template>
    <div class="view_body">
        <Sider :chosen_tab="tab_chosen" v-on:choose_tab="choose_tab" class="sider_bar" />
        <View_content :chosen_tab="tab_chosen" class="view_content"/>
        <Upload_modal :visible.sync="upload_modal_show" :upload_type="upload_type"/>
    </div>
</template>

<script>
    import Sider from "@/components/sider";
    import View_content from "@/components/view_content";
    import Upload_modal from "@/components/upload_modal";
    export default {
        name: "view_body",
        components: {Upload_modal, View_content, Sider},
        data () {
            return {
                tab_chosen: 2,
                upload_modal_show: false,
                upload_type: 0
            }
        },
        methods: {
            choose_tab (index) {
                switch (index) {
                    case 0:
                        this.upload_modal_show = true;
                        this.upload_type = 0;
                        break;
                    case 1:
                        this.upload_modal_show = true;
                        this.upload_type = 1;
                        break;
                    case 2:
                        this.tab_chosen = 2;
                        break;
                    case 3:
                        this.tab_chosen = 3;
                        break;
                    case 4:
                        this.tab_chosen = 4;
                        break;
                }
            }
        }
    }
</script>

<style scoped>
    .sider_bar{
        position: fixed;
        top: 19vh;
        left: 0;
    }
    .view_body{
        position: fixed;
        top: 19vh;
        left: 0;
        width: 100%;
        height: 81vh;
        background: #ededed;
        border: 2px black solid;
    }
    .view_content{
        position: fixed;
        top: 19vh;
        right: 0;
        width: calc(100vw - 350px);
        height: 81vh;
        min-width: 70vw;
    }
</style>