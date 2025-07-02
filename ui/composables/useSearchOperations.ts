import { useSearchState } from "./useSearchState";

export const useSearchOperations = () => {
  const runtimeConfig = useRuntimeConfig();
  const {
    results,
    selectedResultIndex,
    isLoading,
    error,
    searchTime,
    resultCount,
    totalPages,
    currentPage,
    perPage,
  } = useSearchState();

  const loadResults = async (
    queryStr: string,
    page = 1,
    fainderMode?: string,
    resultHighlighting?: boolean,
    fainderIndexName?: string,
  ) => {
    isLoading.value = true;
    error.value = { message: "", details: null };

    console.log(
      `Loading results for query: ${queryStr}, page: ${page}, fainderMode: ${fainderMode}, resultHighlighting: ${resultHighlighting}`,
    );

    try {
      const response = await fetch(`${runtimeConfig.public.apiBase}/query`, {
        method: "POST",
        mode: "cors",
        credentials: "include",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({
          query: queryStr,
          page: page,
          per_page: perPage.value,
          fainder_mode: fainderMode || "low_memory",
          result_highlighting: resultHighlighting,
          fainder_index_name: fainderIndexName || "default",
        }),
      });

      if (!response.ok) {
        const errorBody = await response.text();
        let details;
        try {
          const errorJson = JSON.parse(errorBody);
          details = errorJson.detail || errorJson.message || errorBody;
        } catch {
          details =
            errorBody ||
            `Server returned ${response.status} ${response.statusText}`;
        }

        error.value = {
          message:
            response.status === 500
              ? "Internal Server Error"
              : `Search request failed (${response.status} ${response.statusText})`,
          details: details,
        };
        results.value = null;
        selectedResultIndex.value = 0;
        return;
      }

      const r = await response.json();
      results.value = r.results;
      searchTime.value = r.search_time;
      resultCount.value = r.result_count;
      totalPages.value = r.total_pages;
      currentPage.value = r.page;

      // Only set selectedResultIndex to 0 if it's a new search (page 1)
      // and there's no existing index in the route
      const route = useRoute();
      if (
        page === 1 &&
        !route.query.index &&
        r.results &&
        r.results.length > 0
      ) {
        selectedResultIndex.value = 0;
      }

      return r;
    } catch (e: unknown) {
      const err = e as Error;
      error.value = {
        message: "Failed to perform search",
        details: err.message || String(e),
      };
      results.value = null;
      selectedResultIndex.value = 0; // Reset index on error
    } finally {
      isLoading.value = false;
    }
  };

  return {
    loadResults,
  };
};
