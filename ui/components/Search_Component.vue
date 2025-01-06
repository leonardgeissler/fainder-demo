# Search page for datasets search by precentile predicates and search by key words
# The search page will contain multiple search bars

<template>
  <v-main :class="['search-main', { 'pa-3': inline }]">
    <v-container :class="{ 'pa-2': inline }">
      <v-row :class="{ 'inline-layout': inline }">
        <v-col :cols="inline ? 5 : 12">
          <v-text-field
            v-model="searchPercentilePrecentile"
            label="Search by precentile predicates"
            variant="outlined"
            density="comfortable"
            hide-details
          />
        </v-col>

        <v-col :cols="inline ? 5 : 12">
          <v-text-field
            v-model="searchKeywords"
            label="Search by Keywords"
            variant="outlined"
            density="comfortable"
            hide-details
          />
        </v-col>

        <v-col :cols="inline ? 2 : 12">
          <v-btn 
            @click="searchData"
            :block="!inline"
            :class="{ 'search-btn': inline }"
            color="primary"
            prepend-icon="mdi-magnify"
            variant="elevated"
            size="large"
          >
            Search
          </v-btn>
        </v-col>
      </v-row>
    </v-container>
  </v-main>
</template>


<script setup>
const props = defineProps({
  searchPercentilePrecentile: String,
  searchKeywords: String,
  inline: {
    type: Boolean,
    default: false
  }
});
const emit = defineEmits(['searchData']);

// search bar 1 (search by precentile precentile)
const searchPercentilePrecentile = ref(props.searchPercentilePrecentile);

// search bar 2 (search by Keywords)
const searchKeywords = ref(props.searchKeywords);

async function searchData() {
  console.log(searchPercentilePrecentile.value);
  console.log(searchKeywords.value);

  // emit search data to parent component
  emit('searchData', {
    searchPercentilePrecentile: searchPercentilePrecentile.value,
    searchKeywords: searchKeywords.value
  });
}
</script>

<style scoped>
.search-main {
  background-color: transparent !important;
}

.inline-layout {
  align-items: center;
}

.search-btn {
  height: 48px; /* Match comfortable input height */
}

.v-btn {
  font-weight: 500;
  letter-spacing: 0.5px;
  text-transform: none;
  border-radius: 8px;
  display: flex;
  align-items: center;
}
</style>
