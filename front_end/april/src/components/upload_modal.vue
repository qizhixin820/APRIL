<template>
    <a-modal
            :title="upload_type === 0 ? 'Upload Graph Data' : 'Upload Workload'"
            :visible="visible"
            @ok="handleUpload"
            @cancel="handleCancel"
            :okText="uploading ? 'Uploading' : 'Upload'"
            :okButtonProps="{ props: {loading: uploading, disabled: fileList.length === 0} }"
            cancelText="Cancel"
            :destroyOnClose="true">
        <a-upload
                :fileList="fileList"
                :remove="handleRemove"
                :beforeUpload="beforeUpload">
            <button>Select File</button>
        </a-upload>
    </a-modal>
</template>

<script>
    export default {
        name: "upload_modal",
        props: ['visible', 'upload_type'],
        data() {
            return {
                fileList: [],
                uploading: false,
            }
        },
        methods: {
            handleUpload() {
                const {fileList} = this;
                const formData = new FormData();
                formData.append('type', this.upload_type);
                fileList.forEach(file => {
                    formData.append('files[]', file);
                });
                this.uploading = true;
                this.axiosUpload.post("/upload/upload", formData).then(result => {
                    if (result.data.status === "OK") {
                        this.fileList = [];
                        this.uploading = false;
                        alert(result.data.message);
                        this.$emit('update:visible', false);
                    } else {
                        this.uploading = false;
                        this.$message.error('Upload Failed.');
                    }
                })

                // You can use any AJAX library you like
                // reqwest({
                //     url: 'https://www.mocky.io/v2/5cc8019d300000980a055e76',
                //     method: 'post',
                //     processData: false,
                //     data: formData,
                //     success: () => {
                //         this.fileList = [];
                //         this.uploading = false;
                //         this.$message.success('upload successfully.');
                //         this.$emit('update:visible', false)
                //     },
                //     error: () => {
                //         this.uploading = false;
                //         this.$message.error('upload failed.');
                //     },
                // });
            },
            handleCancel() {
                if (!this.uploading)
                    this.$emit('update:visible', false)
            },
            handleRemove() {
                if (!this.uploading)
                    this.fileList = [];
            },
            beforeUpload(file) {
                this.fileList = [file];
                return false;
            },
        }
    }
</script>

<style scoped>

</style>
