<template>
  <v-app>
    <v-app-bar rounded>
      <Logo size="medium" class="mr-4"/>

      <v-btn @click="gotoHome" prepend-icon="mdi-home">
        Home
      </v-btn>
      
      <v-spacer></v-spacer>

      <v-btn value="theme" @click="toggleTheme">
        <div v-if="theme.global.current.value.dark">
          <v-icon icon="mdi-weather-sunny" color="yellow"></v-icon>
          {{ "Light Mode" }}
        </div>
        <div v-else>
          <v-icon icon="mdi-weather-night" color="indigo"></v-icon> 
          {{ "Dark Mode" }}
        </div>
      </v-btn>
    </v-app-bar>
    <NuxtLayout>
      <NuxtPage />
    </NuxtLayout>
  </v-app>
</template>

<script setup>
  import { useTheme } from 'vuetify'
  import { useRoute } from 'vue-router'
  import Logo from '~/components/Logo.vue'
  
  function gotoHome() {
    console.log('go to home')
    return navigateTo({path:'/'})
  }

  const route = useRoute();
  const theme = useTheme();
  const colorMode = useColorMode();

  let currentTheme = route.query.theme || colorMode.value;
  theme.global.name.value = currentTheme === "dark" ? "dark" : "light";
  
  function toggleTheme() {
    theme.global.name.value = theme.global.name.value === 'dark' ? "light" : "dark";
    
    
    navigateTo({
      path: route.path,
      query: {
        ...route.query,
        theme: theme.global.name.value
      }
    });

  }
</script>
