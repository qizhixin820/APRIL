<template>
    <div class="title_bar">
        <div id="logo">APRIL</div>
        <div id="desc">
            Automatic graph data management system based on reinforcement learning
        </div>
    </div>
</template>

<script>
    export default {
        name: "title_bar"
    }
</script>

<style scoped>
    .title_bar{
        height: 13vh;
        width: 100%;
        background: #1f4e79;
        line-height: 14vh;
        color: white;
    }
    #logo{
        font-size: 7vh;
        display: inline;
        margin-left: 6vw;
        margin-right: 2vw;
        font-family: "HUAWEIXINWEI", sans-serif;
        font-weight: bold;
    }
    #desc{
        font-size: 3.5vh;
        display: inline;
        font-weight: bold;
    }
</style>