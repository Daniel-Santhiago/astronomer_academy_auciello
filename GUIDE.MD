# Astronomer Registry
registry.astronomer.io

# Deploy Astro CLI
para realizar o deploy para o Astro CLI é preciso logar-se pelo terminal Power Shell

```
astro login
```

O deploy pode ser realizado pelo comando 
```
astro deploy
```
Este comando porém não é a única alternativa.
Dentro da interface Web do Astro após feito o login navegue:
Deployments > selecione o Deployment > Algumas abas aparecerão > Selecione Access > Selecione a aba API Tokens > Add API Key no canto direito botão azul 
Salve o API Token em algum bloco de notas
Salve o token em uma variavel
```
export ASTRO_API_TOKEN=<your-token>
```
ou 
```
set ASTRO_API_TOKEN=<your-token>
```

Vá no ambiente cloud do Astronomer e copie o seu id de deployment, não confundir com o id do Workspace!
```
astro deploy clweb3j0r0xhc01il5wz96hxa --pytest
```