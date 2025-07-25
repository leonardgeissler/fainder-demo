<template>
  <v-app>
    <v-app-bar :elevation="0" height="85">
      <FainderLogo
        size="medium"
        class="mr-4 ml-4 clickable"
        @click="gotoHome"
      />
      <v-spacer />

      <!-- Add search component in app bar only on results page -->
      <template v-if="route.path === '/results'">
        <Search_Component
          :key="searchComponentKey"
          :search-query="internalSearchQuery"
          :inline="true"
          :lines="1"
          :query-builder="false"
          class="app-bar-search"
          @search-data="searchData"
        />
        <v-btn
          icon
          density="compact"
          class="ml-2"
          @click="showSearchDialog = true"
        >
          <v-icon>mdi-arrow-expand</v-icon>
        </v-btn>
      </template>

      <v-spacer />

      <v-menu class="mr">
        <template #activator="{ props }">
          <v-btn icon v-bind="props">
            <v-icon>mdi-menu</v-icon>
          </v-btn>
        </template>

        <v-list>
          <v-list-item @click="navigateTo('/upload')">
            <template #prepend>
              <v-icon icon="mdi-upload" />
            </template>
            <v-list-item-title>Upload Datasets</v-list-item-title>
          </v-list-item>

          <v-list-item @click="navigateTo('/about')">
            <template #prepend>
              <v-icon icon="mdi-information" />
            </template>
            <v-list-item-title>About</v-list-item-title>
          </v-list-item>

          <v-divider class="mx-3" />

          <v-list-item @click="toggleHighlight">
            <template #prepend>
              <v-icon icon="mdi-marker" />
            </template>
            <v-list-item-title>
              {{
                syntaxHighlighting
                  ? "Disable Syntax Highlighting"
                  : "Enable Syntax Highlighting"
              }}
            </v-list-item-title>
          </v-list-item>

          <v-list-item @click="toggleTheme">
            <template #prepend>
              <v-icon
                :icon="
                  theme.global.current.value.dark
                    ? 'mdi-weather-sunny'
                    : 'mdi-weather-night'
                "
                :color="theme.global.current.value.dark ? 'yellow' : 'indigo'"
              />
            </template>
            <v-list-item-title>
              {{ theme.global.current.value.dark ? "Light Mode" : "Dark Mode" }}
            </v-list-item-title>
          </v-list-item>
        </v-list>
      </v-menu>
    </v-app-bar>

    <!-- Add expandable search dialog -->
    <v-dialog
      v-model="showSearchDialog"
      transition="dialog-top-transition"
      max-width="56rem"
    >
      <v-card elevation="0">
        <v-toolbar dark color="primary">
          <v-btn icon dark @click="showSearchDialog = false">
            <v-icon>mdi-close</v-icon>
          </v-btn>
          <v-toolbar-title>Query Builder</v-toolbar-title>
        </v-toolbar>

        <v-container class="pt-6">
          <Search_Component
            :key="searchComponentKey"
            :search-query="route.query.query"
            :inline="false"
            :lines="6"
            :query-builder="false"
            :simple-builder="true"
            @search-data="
              (data: SearchParams) => {
                searchData(data);
                showSearchDialog = false;
              }
            "
          />
        </v-container>
      </v-card>
    </v-dialog>

    <NuxtLayout>
      <NuxtPage />
    </NuxtLayout>
  </v-app>
</template>

<script setup lang="ts">
import { useTheme } from "vuetify";
import { useRoute } from "vue-router";

interface SearchParams {
  query: string;
  fainderMode: string;
  resultHighlighting: boolean;
  fainderIndexName: string;
}

const { loadResults } = useSearchOperations();
const route = useRoute();
const theme = useTheme();
const { query, fainderMode, currentPage, selectedResultIndex } =
  useSearchState();
const colorMode = useColorMode();

const syntaxHighlighting = useCookie("fainderSyntaxHighlighting", {
  default: () => true,
});

const internalSearchQuery = computed(
  () => route.query.query as string | undefined,
);
const searchComponentKey = ref(0);

const currentTheme = route.query.theme || colorMode.value;
theme.global.name.value = currentTheme === "dark" ? "dark" : "light";

function gotoHome(): ReturnType<typeof navigateTo> {
  console.log("go to home");
  // keep everything in the query except the query string
  return navigateTo({
    path: "/",
    query: { ...route.query, query: undefined, theme: theme.global.name.value },
  });
}

function toggleTheme(): void {
  theme.global.name.value =
    theme.global.name.value === "dark" ? "light" : "dark";

  navigateTo({
    path: route.path,
    query: {
      ...route.query,
      theme: theme.global.name.value,
    },
  });
}

function toggleHighlight(): void {
  syntaxHighlighting.value = !syntaxHighlighting.value;
}

const showSearchDialog = ref(false);

async function searchData({
  query: searchQuery,
  fainderMode: newFainderMode,
  resultHighlighting,
  fainderIndexName,
}: SearchParams): Promise<void> {
  query.value = searchQuery;
  fainderMode.value = newFainderMode;

  currentPage.value = 1;
  selectedResultIndex.value = 0;

  await loadResults(
    searchQuery,
    1,
    newFainderMode,
    resultHighlighting,
    fainderIndexName,
  );

  await navigateTo({
    path: "/results",
    query: {
      query: searchQuery,
      page: 1,
      index: 0,
      fainderMode: newFainderMode,
      resultHighlighting: String(resultHighlighting),
      theme: theme.global.name.value,
      fainderIndexName: fainderIndexName,
    },
  });

  searchComponentKey.value++;
}
</script>

<style scoped>
.app-bar-search {
  max-width: 1200px; /* Increased from 800px */
  flex-grow: 1;
}

.app-bar-search :deep(.v-field) {
  border-radius: 20px;
}

.clickable {
  cursor: pointer;
}
</style>
