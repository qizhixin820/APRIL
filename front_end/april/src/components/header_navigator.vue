<template>
    <div class="header_navigator">
        <div id="links">
            <a class="navigator_btn">Home</a>
            <s class="separator">|</s>
            <a class="navigator_btn">Language</a>
            <s class="separator">|</s>
            <a class="navigator_btn">About</a>
            <s class="separator">|</s>
            <a class="navigator_btn">Contacts</a>
        </div>
    </div>
</template>

<script>
    export default {
        name: "header_navigator"
    }
</script>

<style scoped>
    .header_navigator{
        font-size: 2.2vh;
        line-height: 5.2vh;
        background: #222a35;
        height: 6vh;
        width: 100%;
    }
    .navigator_btn{
        color: white;
        margin: 2vh;
    }
    .separator{
        color: white;
    }
    #links{
        margin-left: 2vw;
    }
</style>