# Search page for datasets search by precentile predicates and search by key words
# The search page will contain multiple search bars

<template>
  <v-main :class="['search-main', { 'pa-3': inline }]">
    <v-container :class="{ 'pa-2': inline }">
      <v-row :class="{ 'inline-layout': inline }">
        <v-col :cols="inline ? 10 : 12">
          <v-text-field
            v-model="searchQuery"
            label="Search by percentile predicates and keywords"
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
            @keyup.enter="searchData"
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
  searchQuery: String,
  inline: {
    type: Boolean,
    default: false
  }
});
const emit = defineEmits(['searchData']);

const searchQuery = ref(props.searchQuery);

async function searchData() {
  emit('searchData', {
    query: searchQuery.value
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
