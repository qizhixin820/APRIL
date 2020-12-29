<template>
    <div id="app">
        <Header class="header_navigator"/>
        <Title_bar class="title_bar"/>
        <View_body/>
    </div>
</template>

<script>
    import Header from "@/components/header_navigator";
    import Title_bar from "@/components/title_bar";
    import View_body from "@/components/view_body";
    export default {
        name: "index",
        components: {View_body, Title_bar, Header}
    }
</script>

<style scoped>
    #app{
        font-family: "Arial",   sans-serif;
        width: 100%;
        height: 100%;
    }
    .header_navigator{
        position: fixed;
        left: 0;
        top: 0;
    }
    .title_bar{
        position: fixed;
        top: 6vh;
        left: 0;
    }
</style>