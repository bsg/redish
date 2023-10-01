use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::fd::AsRawFd,
};

macro_rules! syscall {
    ($fn: ident ( $($arg: expr),* $(,)* ) ) => {{
        let res = unsafe { libc::$fn($($arg, )*) };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

struct Client {
    stream: TcpStream,
    buf: [u8; 1024],
    nread: usize,
}

fn epoll_add_interest(epoll_fd: libc::c_int, fd: libc::c_int, events: i32) {
    syscall!(epoll_ctl(
        epoll_fd,
        libc::EPOLL_CTL_ADD,
        fd,
        &mut libc::epoll_event {
            events: events as u32,
            u64: fd as u64,
        }
    ))
    .unwrap();
}

fn epoll_mod_interest(epoll_fd: libc::c_int, fd: libc::c_int, events: i32) {
    syscall!(epoll_ctl(
        epoll_fd,
        libc::EPOLL_CTL_MOD,
        fd,
        &mut libc::epoll_event {
            events: events as u32,
            u64: fd as u64,
        }
    ))
    .unwrap();
}

fn main() {
    let mut db: HashMap<String, String> = HashMap::new();
    let mut clients: HashMap<u64, Client> = HashMap::new();

    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener_fd = listener.as_raw_fd();

    let epoll_fd = syscall!(epoll_create1(0)).unwrap();
    if let Ok(flags) = syscall!(fcntl(listener_fd, libc::F_GETFD)) {
        let _ = syscall!(fcntl(listener_fd, libc::F_SETFD, flags | libc::FD_CLOEXEC));
    }

    epoll_add_interest(epoll_fd, listener_fd, libc::EPOLLIN | libc::EPOLLONESHOT);

    let mut events: Vec<libc::epoll_event> = Vec::with_capacity(1024);

    loop {
        events.clear();
        let res = match syscall!(epoll_wait(
            epoll_fd,
            events.as_mut_ptr() as *mut libc::epoll_event,
            1024,
            1000 as libc::c_int,
        )) {
            Ok(v) => v,
            Err(e) => panic!("error during epoll wait: {}", e),
        };

        unsafe { events.set_len(res as usize) };

        for event in &events {
            let key = event.u64;
            if key == listener_fd as u64 {
                match listener.accept() {
                    Ok((mut stream, addr)) => {
                        println!("Accepted connection from {}", addr);
                        stream.set_nonblocking(true).unwrap();

                        epoll_add_interest(
                            epoll_fd,
                            stream.as_raw_fd(),
                            libc::EPOLLIN | libc::EPOLLONESHOT,
                        );
                        epoll_mod_interest(
                            epoll_fd,
                            listener_fd,
                            libc::EPOLLIN | libc::EPOLLONESHOT,
                        );

                        stream.write_all("> ".as_bytes()).unwrap();

                        clients.insert(
                            stream.as_raw_fd() as u64,
                            Client {
                                stream,
                                buf: [0u8; 1024],
                                nread: 0,
                            },
                        );
                    }
                    Err(_) => todo!(),
                }
            } else {
                let client = clients.get_mut(&key).unwrap();

                match client.stream.read(&mut client.buf[client.nread..]) {
                    Ok(nread) => {
                        client.nread += nread;
                        if nread > 0
                            && client.buf[client.nread - 2] == b'\r'
                            && client.buf[client.nread - 1] == b'\n'
                        {
                            handle_command(&mut db, client);
                            client.nread = 0;
                            client.stream.write_all("> ".as_bytes()).unwrap();
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => todo!(),
                }

                syscall!(epoll_ctl(
                    epoll_fd,
                    libc::EPOLL_CTL_MOD,
                    client.stream.as_raw_fd(),
                    &mut libc::epoll_event {
                        events: (libc::EPOLLIN | libc::EPOLLONESHOT) as u32,
                        u64: client.stream.as_raw_fd() as u64,
                    }
                ))
                .unwrap();
            }
        }
    }
}

fn handle_command(db: &mut HashMap<String, String>, client: &mut Client) {
    let cmd = std::str::from_utf8(&client.buf[..client.nread - 2])
        .unwrap()
        .to_string();
    if cmd.starts_with("set ") {
        let mut split = cmd.splitn(3, ' ');
        split.next();
        let key = match split.next() {
            Some(s) => s.to_string(),
            None => {
                client.stream.write("ERR key\n".as_bytes()).unwrap();
                return;
            }
        };
        let value = match split.next() {
            Some(s) => s.to_string(),
            None => {
                client.stream.write("ERR value\n".as_bytes()).unwrap();
                return;
            }
        };

        println!("SET {} {}\n", key, value);

        db.insert(key, value);
        client.stream.write("OK\n".as_bytes()).unwrap();
    } else if cmd.starts_with("get ") {
        let mut split = cmd.splitn(2, ' ');
        split.next();
        let key = match split.next() {
            Some(s) => s.to_string(),
            None => {
                client.stream.write("ERR key\n".as_bytes()).unwrap();
                return;
            }
        };

        println!("GET {}\n", key);

        let resp = match db.get(&key) {
            Some(value) => value,
            None => "ERR\n",
        };
        client.stream.write(resp.as_bytes()).unwrap();
        client.stream.write("\r\n".as_bytes()).unwrap();
    } else {
        client.stream.write("ERR\n".as_bytes()).unwrap();
    }
}
