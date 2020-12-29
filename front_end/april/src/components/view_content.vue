<template>
    <div class="view_content">
      <View_content_settings :user_specific="user_spcecific_opening" :load_file="load_file" :chosen_tab="chosen_tab" style=" margin-top:2%; width: 90%; margin-left: 5%"/>
      <Result_box :chosen_tab="chosen_tab" :file_text="file_text" v-show="!user_spcecific_opening" v-on:update:user_spcecific_opening="user_spcecific_opening=$event" style="width: 90%; margin-left: 5%"/>
      <User_specific :chosen_tab="chosen_tab" v-on:close="user_spcecific_opening=false" v-show="user_spcecific_opening" style="width: 90%; margin-left: 5%"/>
    </div>
</template>

<script>
    import View_content_settings from "@/components/view_content_settings";
    import Result_box from "@/components/result_box";
    import User_specific from "@/components/user_specific";
    export default {
        name: "view_content",
        components: {User_specific, Result_box, View_content_settings},
        props: ['chosen_tab'],
        data() {
            return {
                user_spcecific_opening: false,
                file_text:""
            }
        },
        watch: {
            'chosen_tab': function () {
                this.user_spcecific_opening = false
            }

    },
      methods: {
        load_file(FILE_NAME) {
          let params = new FormData();
          console.log(FILE_NAME);
          params.append("file_name", FILE_NAME);
          this.axiosJSON.post("/storage/get_file_context", params).then(result => {
            if (result.data.status === "OK") {
              console.log("load finish");
              console.log(result.data.result);
              this.file_text = result.data.result;
              console.log(this.file_text);
            } else {
              alert(result.data.message);
            }
          })
        }
        }
    }
</script>

<style scoped>
    .view_content{
        font-size: 3vh;
    }
</style>

