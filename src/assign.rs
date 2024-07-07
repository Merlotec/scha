extern crate nalgebra as na;
use std::{collections::HashMap, f64::consts::PI};

use itertools::Itertools;
use nalgebra::{Vector, Vector2};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Circle {
    pub origin: Vector2<f64>,
    pub r: f64,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Intersection {
    Inside(Circle),
    Intersect(Vector2<f64>, Vector2<f64>, bool),
    None,
}

impl Circle {
    fn new(x: f64, y: f64, r: f64) -> Circle {
        Circle { origin: Vector2::new(x, y), r }
    }

    fn distance(&self, other: &Circle) -> f64 {
        self.origin.metric_distance(&other.origin)
    }

    fn intersection_area(&self, other: &Circle) -> f64 {
        let d = self.distance(other);

        if d >= self.r + other.r {
            // No intersection
            return 0.0;
        }

        if d <= (self.r - other.r).abs() {
            // One circle is completely inside the other
            return PI * self.r.min(other.r).powi(2);
        }

        let r1_squared = self.r.powi(2);
        let r2_squared = other.r.powi(2);

        let angle1 = (r1_squared + d.powi(2) - r2_squared) / (2.0 * self.r * d);
        let angle2 = (r2_squared + d.powi(2) - r1_squared) / (2.0 * other.r * d);

        let theta1 = 2.0 * angle1.acos();
        let theta2 = 2.0 * angle2.acos();

        let segment1 = 0.5 * r1_squared * (theta1 - theta1.sin());
        let segment2 = 0.5 * r2_squared * (theta2 - theta2.sin());

        segment1 + segment2
    }

    pub fn intersect(&self, other: &Circle) -> Intersection {
        let d = self.distance(other);

        if d > self.r + other.r {
            Intersection::None
        } else if d + other.r < self.r {
            Intersection::Inside(*other)
        } else if d + self.r < other.r {
            Intersection::Inside(*self)
        } else {
            let r_sq = self.r * self.r;
            let d_sq = d * d;
            let v = (r_sq + d_sq - other.r * other.r) / 2.0 * d;
            let h_sq = self.r * self.r - v * v;
            let h = h_sq.sqrt();

            let s_sq = r_sq - h_sq;

            let s = s_sq.sqrt();

            let l = other.origin - self.origin;

            let lnorm = l.normalize();

            let mp = lnorm * s;

            let lperp = Vector2::new(-lnorm.y, lnorm.x);

            let a = mp + lperp * h;
            let b = mp + lperp * (-h);

            let nearside = s > d;

            Intersection::Intersect(a, b, nearside)
        }
    }

    pub fn union_all(circles: &[Circle]) -> f64 {
        let mut points: Vec<(Vector2<f64>, usize, usize)> = Vec::new();
        let mut ignores: Vec<usize> = Vec::new();
        for ((i, c1), (j, c2)) in circles.iter().enumerate().cartesian_product(circles.iter().enumerate()) {
            match c1.intersect(c2) {
                Intersection::Intersect(a, b) => {
                    points.push((a, i, j));
                    points.push((b, i, j));
    
                },
                Intersection::Inside(cx) => {
                    if cx == *c1 {
                        ignores.push(j);
                    } else {
                        ignores.push(i);
                    }
                },
                Intersection::None => {}, 
            }
        }

        // only keep intersections in inner most area.
        points.retain(|(p, i, j)| {
            if ignores.contains(i) || ignores.contains(j) {
                return false;
            }

            let mut count = 0;
            for (k, c) in circles.iter().enumerate() {
                if k != *i && k != *j {
                    if p.metric_distance(&c.origin) <= c.r {
                        count += 1;
                    }
                }
            }
            count == circles.len() - 2 
        });

        // calculate inner polygon area. 
        let poly_area = polygon_area(points.iter().map(|(p, _, _)| *p).collect::<Vec<Vector2<f64>>>().as_slice());

        // since our shape is convex we can use the 'centre of mass' of the points to determine directions of the normals of the faces, because the centre of mass will lie in the shape for convex shapes.
        let cm = points.iter().fold(Vector2::zeros(), |x, (p, _, _)| x + p) / points.len() as f64;

        for (c, circle) in circles.iter().enumerate() {
            // Get line segment for circle.
            if !ignores.contains(&c) {
                let mut p1 = None;
                let mut p2 = None;
                for (p, i, j) in points.iter() {
                    if *i == c || *j == c {
                        if p1.is_none() {
                            p1 = Some(*p);
                        } else {
                            p2 = Some(*p);
                        }
                    }
                }

                let p1 = p1.unwrap();
                let p2 = p2.unwrap();

                let v = p2 - p1;
                let l = v.norm();
                let vn = v.normalize();
                let n1 = Vector2::new(-vn.y, vn.x);

                let vcm = cm - p1;

                let segnorm = if vcm.dot(&n1) < 0.0 {
                    n1
                } else {
                    -n1
                };

                let cv = circle.origin - p1;

                if cv.dot(&segnorm) <= 0.0 {
                    // Usual situation - smaller segment of the circle to be added.
                    segment_area(circle.r, l);
                } else {
                    // Unusual situation - larger segment of the circle to be added.
                    PI * circle.r * circle.r - segment_area(circle.r, l); // Add half of the circle
                }
            }
        }

        for i in 0..points.len() {
            let j = (i + 1) % points.len();
            
        }

        0.0
    }

    pub fn intersect_all(&self, others: &[Circle]) -> f64 {
        let mut a: f64 = 0.0;
        for layer in 0.. {

        }

        0.0
    }
}


fn segment_area(r: f64, l: f64) -> f64 {
    if l > 2.0 * r {
        panic!("Chord length cannot be greater than the diameter of the circle");
    }

    let h = r - ((r * r - (l * l) / 4.0).sqrt());
    let theta = 2.0 * ((l / (2.0 * r)).asin());
    let area = 0.5 * r * r * (theta - theta.sin());

    area
}

fn polygon_area(vertices: &[Vector2<f64>]) -> f64 {
    let n = vertices.len();
    if n < 3 {
        return 0.0; // Not a polygon
    }

    let mut area = 0.0;
    for i in 0..n {
        let j = (i + 1) % n;
        area += vertices[i].x * vertices[j].y;
        area -= vertices[j].x * vertices[i].y;
    }

    (area / 2.0).abs()
}


pub fn resize(circle: Circle, others: Vec<Circle>) -> Circle {

}