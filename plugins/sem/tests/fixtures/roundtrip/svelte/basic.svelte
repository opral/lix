<script lang="ts">
  export let name: string = "Ada";

  function greeting(value: string): string {
    return `Hello, ${value}!`;
  }
</script>

<h1>{greeting(name)}</h1>

<style>
  h1 {
    color: rebeccapurple;
  }
</style>
