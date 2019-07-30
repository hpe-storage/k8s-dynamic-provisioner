FROM scratch
ADD [ "k8s-dynamic-provisioner", "/usr/local/bin/k8s-dynamic-provisioner" ]
ENTRYPOINT [ "k8s-dynamic-provisioner" ]
CMD [ "/etc/kubernetes/admin.conf", "hpe.com" ]
