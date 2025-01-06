<template>
  <v-main class="d-flex align-center justify-center">
    <div class="search-wrapper text-center">
      <Logo size="large" class="mb-8"/>
      <Search_Component 
        :searchPercentilePrecentile="percentile" 
        :searchKeywords="keywords" 
        @searchData="searchData" 
      />
    </div>
  </v-main>
</template>

<script setup>
import Logo from '~/components/Logo.vue'

const route = useRoute();
const q = route.query;

const keywords = ref(q.keywords);
const percentile = ref(q.percentile);


async function searchData({searchPercentilePrecentile, searchKeywords}) {
  console.log('searchData');
  console.log(searchPercentilePrecentile);
  console.log(searchKeywords);

  return await navigateTo({path: '/results', query: {percentile: searchPercentilePrecentile, keywords: searchKeywords}});
}

</script>

<style scoped>
.search-wrapper {
  width: 100%;
  max-width: 800px;
  padding: 24px;
}
</style>
