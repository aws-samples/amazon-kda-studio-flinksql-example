FROM public.ecr.aws/nginx/nginx:latest
COPY nginx-default.conf.template /etc/nginx/conf.d/default.conf.template
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
RUN ["chmod", "+x", "/docker-entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]
